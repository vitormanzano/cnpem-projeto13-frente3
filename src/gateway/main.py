import asyncio
import contextlib
import json
import logging
import os
import socket
import time
from collections.abc import Callable
from dataclasses import fields
from datetime import datetime, timedelta

from dotenv import load_dotenv

from bluetooth.conexao import ConexaoPulseira
from bluetooth.domain.erros import (
    BrokerIndisponivel, FalhaConexao, GatewayError, PulseiraNaoEncontrada, RSSIIndisponivel,
)
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI
from bluetooth.reconexao import manter_conexao
from bluetooth.scanner import VarreduraPulseira, find_all_local_devices
from coletor.coletor import identificador_coletor
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import CONFIG_POR_TIPO, VERSAO_FORMATO, EstadoPresenca, Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from mqtt.presenca import PresencaColetor

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

UUID_SERVICO_FC = "0000180d-0000-1000-8000-00805f9b34fb"
ENDERECO_INEXISTENTE = "00:11:22:33:44:55"
PULSEIRA_FICTICIA = "AA:BB:CC:DD:EE:FF"


# ------------------------------------------------------------------ utilitários dos testes

_falhas: list[str] = []


def _verificar(descricao: str, ok: bool) -> bool:
    """Registra um critério de aceitação como [OK] ou [FALHA]; as falhas entram no resumo final."""
    if ok:
        logging.info(f"[OK]    {descricao}")
    else:
        logging.error(f"[FALHA] {descricao}")
        _falhas.append(descricao)
    return ok


def _config(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise SystemExit(f"Defina a variável de ambiente {nome} (veja .env.example)")
    return valor


async def _aguardar(condicao: Callable[[], bool], timeout: float) -> bool:
    """Espera a condição ficar verdadeira. Devolve False se o tempo esgotar."""
    limite = time.monotonic() + timeout
    while not condicao():
        if time.monotonic() >= limite:
            return False
        await asyncio.sleep(0.1)
    return True


async def _encerrar(*tarefas: asyncio.Task) -> None:
    for tarefa in tarefas:
        tarefa.cancel()
    await asyncio.gather(*tarefas, return_exceptions=True)


async def _aguardar_primeira_conexao(conexao: ConexaoPulseira, laco: asyncio.Task) -> None:
    """Espera o manter_conexao conectar; se ele falhar antes, propaga o erro (RF03)."""
    await _aguardar(lambda: conexao.conectado or laco.done(), timeout=conexao.timeout_conexao + 5)
    if laco.done():
        await laco


async def _primeira_leitura(endereco: str, timeout: float = 10) -> LeituraRSSI | None:
    """Roda a varrudera até a primeira leitura: desconectado (achou) ou nao-encontrado."""
    leituras: list[LeituraRSSI] = []
    varredura = VarreduraPulseira(endereco, tempo_ausencia=timeout)
    varredura.ao_ler = leituras.append
    tarefa = asyncio.create_task(varredura.executar())
    await _aguardar(lambda: bool(leituras), timeout=timeout + varredura.intervalo + 1)
    await _encerrar(tarefa)
    return leituras[0] if leituras else None


async def _tentar_assinar_fc(conexao: ConexaoPulseira) -> bool:
    try:
        await conexao.iniciar_leitura_frequencia_cardiaca()
        return True
    except Exception as e:  # noqa: BLE001 - o teste segue sem FC e mostra o motivo
        logging.warning(f"Frequência cardíaca não assinada [{type(e).__name__}]: {e}")
        return False


async def _espiao(*topicos: str, recebidas: list[Mensagem] | None = None) -> ConexaoBroker:
    """Segunda sessão MQTT que só assina e mostra o que o broker entrega, para conferir as publicações."""
    def ao_receber(topico: str, mensagem: Mensagem) -> None:
        if recebidas is not None:
            recebidas.append(mensagem)
        logging.info(f"[espião] {topico} | {mensagem.para_json()}")

    espiao = ConexaoBroker(cliente_id=f"{identificador_coletor()}-espiao-{os.getpid()}", ao_receber=ao_receber)
    for topico in topicos:
        await espiao.assinar(topico)
    return espiao


def _derrubar_sem_aviso(conexao: ConexaoBroker) -> None:
    """Simula a queda do processo: corta o TCP sem mandar DISCONNECT, então o broker publica a última vontade.

    Mexe no interior do aiomqtt/paho; serve só para teste.
    """
    conexao._client._client.socket().shutdown(socket.SHUT_RDWR)


async def espiar(*topicos: str) -> None:
    """Mostra tudo o que chega nos tópicos até Ctrl+C. Útil num segundo terminal: espiar("#")."""
    espiao = await _espiao(*(topicos or ("#",)))
    await espiao.manter_conexao()


# ------------------------------------------------------------------ BLE

async def teste_rf03() -> None:
    """RF03 - Conectar à pulseira: conecta, consulta estado e serviços, libera e testa a falha com endereço inexistente."""
    conexao = ConexaoPulseira(_config("TARGET_ADDRESS"))
    inicio = time.monotonic()
    await conexao.conectar()
    try:
        duracao = time.monotonic() - inicio
        _verificar(f"conectou em {duracao:.1f}s (limite {conexao.timeout_conexao:g}s)", duracao <= conexao.timeout_conexao)
        _verificar("estado consultável: conectado", conexao.estado is EstadoConexao.CONECTADO and conexao.conectado)
        _verificar(f"serviços disponíveis ({len(conexao.servicos)})", len(conexao.servicos) > 0)
    finally:
        await conexao.desconectar()
    _verificar("encerramento controlado: desconectado", conexao.estado is EstadoConexao.DESCONECTADO and not conexao.conectado)

    ausente = ConexaoPulseira(ENDERECO_INEXISTENTE, timeout_conexao=5)
    try:
        await ausente.conectar()
        _verificar("endereço inexistente gera falha-conexao", False)
    except FalhaConexao as e:
        _verificar(f"endereço inexistente gera {e.codigo} com motivo ('{e}')", bool(str(e)))


async def teste_rf07() -> None:
    """RF07 - Descobrir serviços: mensagem informativa sem conexão, depois UUID e descrição de cada serviço."""
    conexao = ConexaoPulseira(_config("TARGET_ADDRESS"))
    try:
        _ = conexao.servicos
        _verificar("sem conexão: mensagem informativa", False)
    except FalhaConexao as e:
        _verificar(f"sem conexão: mensagem informativa ('{e}')", True)

    async with conexao:
        servicos = conexao.servicos
        for servico in servicos:
            logging.info(f"Serviço {servico.uuid} | {servico.descricao}")
        _verificar("todo serviço tem UUID e descrição", all(s.uuid and s.descricao for s in servicos))
        _verificar("serviço de frequência cardíaca (0x180D) na lista",
                   any(s.uuid.lower() == UUID_SERVICO_FC for s in servicos))


async def teste_rf04(duracao: float = 30) -> None:
    """RF04 - Ler RSSI por `duracao` s junto com a frequência cardíaca. O intervalo vem de RSSI_INTERVALO."""
    async with ConexaoPulseira(_config("TARGET_ADDRESS")) as conexao:
        fc_ativa = await _tentar_assinar_fc(conexao)
        monitor = asyncio.create_task(conexao.monitorar_rssi())
        await asyncio.wait({monitor}, timeout=duracao)
        parou_sozinho = monitor.done()
        await _encerrar(monitor)

        leituras = conexao.registros_rssi
        _verificar("monitor rodou até o fim (não parou sozinho)", not parou_sozinho)
        _verificar("leituras com RSSI em dBm e horário", bool(leituras) and all(leitura.rssi is not None for leitura in leituras))
        if len(leituras) >= 2:
            passos = [(b.timestamp - a.timestamp).total_seconds() for a, b in zip(leituras, leituras[1:])]
            logging.info(
                f"Intervalo medido: {sum(passos) / len(passos):.2f}s "
                f"(configurado: {os.getenv('RSSI_INTERVALO', '1')}s)"
            )
        _verificar("conexão continua ativa depois das leituras", conexao.conectado)
        if fc_ativa:
            _verificar("frequência cardíaca continuou chegando", bool(conexao.registros_frequencia))


async def teste_rf16(duracao: float = 30) -> None:
    """RF16 - Receber a frequência cardíaca por `duracao` s, com o RSSI rodando junto.

    Para o aviso de "sem leitura por 5 s", tire a pulseira do braço durante o teste e confira o log.
    A reativação após reconexão aparece no teste_rf05.
    """
    async with ConexaoPulseira(_config("TARGET_ADDRESS")) as conexao:
        _verificar("serviço de frequência cardíaca disponível",
                   any(s.uuid.lower() == UUID_SERVICO_FC for s in conexao.servicos))
        if not _verificar("notificações de FC (0x2A37) ativadas", await _tentar_assinar_fc(conexao)):
            return
        logging.info(
            f"Campos de cada leitura: {[f.name for f in fields(LeituraFrequenciaCardiaca)]} "
            "(o RF16 pede bpm, intervalos RR e horário)"
        )

        monitor = asyncio.create_task(conexao.monitorar_rssi())
        ultima = None
        recebidas = 0
        limite = time.monotonic() + duracao
        while time.monotonic() < limite and conexao.conectado:
            await asyncio.sleep(1)
            registros = conexao.registros_frequencia
            if registros and registros[-1] is not ultima:
                ultima = registros[-1]
                recebidas += 1
                logging.info(f"FC {ultima.bpm} bpm às {ultima.timestamp:%H:%M:%S.%f}")
        await _encerrar(monitor)
        if conexao.conectado:
            await conexao.parar_leitura_frequencia_cardiaca()

        _verificar(f"recebeu frequência cardíaca ({recebidas} leitura(s), amostradas a cada 1 s)", recebidas > 0)
        _verificar("RSSI lido em paralelo", bool(conexao.registros_rssi))
        _verificar("conexão continua ativa", conexao.conectado)


async def teste_rf08(esperar_queda: bool = False) -> None:
    """RF08 - Estado da conexão: nao-encontrado, conectado e desconectado (controlado ou por queda).

    Com esperar_queda=True, depois de conectar desligue ou afaste a pulseira.
    Cada mudança deve aparecer no log com horário, estado anterior, estado novo e motivo.
    """
    try:
        leitura = await _primeira_leitura(ENDERECO_INEXISTENTE, timeout=5)
        _verificar("varredura sem a pulseira: nao-encontrado", 
                   leitura is not None and leitura.estado is EstadoConexao.NAO_ENCONTRADO)
    except PulseiraNaoEncontrada:
        _verificar("varredura sem a pulseira: nao-encontrado", True)

    ausente = ConexaoPulseira(ENDERECO_INEXISTENTE, timeout_conexao=5)
    with contextlib.suppress(FalhaConexao):
        await ausente.conectar()
    _verificar("conexão a pulseira ausente: nao-encontrado", ausente.estado is EstadoConexao.NAO_ENCONTRADO)

    conexao = ConexaoPulseira(_config("TARGET_ADDRESS"))
    _verificar("estado inicial: desconectado", conexao.estado is EstadoConexao.DESCONECTADO)
    await conexao.conectar()
    try:
        _verificar("após conectar: conectado", conexao.estado is EstadoConexao.CONECTADO)
        if esperar_queda:
            logging.info("Aguardando a queda: desligue ou afaste a pulseira (Ctrl+C para desistir)")
            await conexao.aguardar_desconexao()
            _verificar("após a queda: desconectado, não intencional",
                       conexao.estado is EstadoConexao.DESCONECTADO and not conexao.desconexao_intencional)
        else:
            await conexao.desconectar()
            _verificar("após desconectar: desconectado, intencional",
                       conexao.estado is EstadoConexao.DESCONECTADO and conexao.desconexao_intencional)
    finally:
        await conexao.desconectar()


async def teste_rf10(conectada_por: float = 10) -> None:
    """RF10 - Desconectar: com manter_conexao rodando, desconecta, confere que não reconecta e que a pulseira volta a anunciar.

    Para "o encerramento do coletor libera a conexão", aperte Ctrl+C enquanto estiver conectada:
    deve aparecer "Desconectando da pulseira" no log.
    O estado desconectado publicado no broker é conferido no teste_rf09.
    """
    endereco = _config("TARGET_ADDRESS")
    conexao = ConexaoPulseira(endereco)
    laco = asyncio.create_task(manter_conexao(conexao))
    try:
        await _aguardar_primeira_conexao(conexao, laco)
        await asyncio.sleep(conectada_por)
        await conexao.desconectar()
        encerrou = await _aguardar(laco.done, timeout=5)
        _verificar("desconexão intencional não disparou reconexão", encerrou and not conexao.conectado)
        _verificar("estado desconectado", conexao.estado is EstadoConexao.DESCONECTADO)
    finally:
        await _encerrar(laco)
        await conexao.desconectar()

    await asyncio.sleep(2)  # tempo para a pulseira voltar a anunciar
    try:
        leitura = await _primeira_leitura(endereco)
        _verificar("pulseira voltou a aparecer na varredura", 
                   leitura is not None and leitura.estado is EstadoConexao.NAO_ENCONTRADO)
    except PulseiraNaoEncontrada:
        _verificar("pulseira voltou a aparecer na varredura", False)


async def teste_rf05(desconectar_apos: float | None = None) -> None:
    """RF05 - Reconexão automática. Roda manter_conexao até esgotar as tentativas ou Ctrl+C.

    Provoque quedas afastando ou desligando a pulseira: cada queda e reconexão deve aparecer no log com
    horário, e o RSSI deve voltar após reconectar. Para esgotar, deixe-a fora por mais de
    RECONEXAO_MAX_TENTATIVAS x (CONNECT_TIMEOUT + RECONEXAO_INTERVALO).
    Com desconectar_apos, faz uma desconexão intencional nesse tempo: o laço deve terminar sem reconectar.
    """
    conexao = ConexaoPulseira(_config("TARGET_ADDRESS"))
    laco = asyncio.create_task(manter_conexao(conexao))
    try:
        await _aguardar_primeira_conexao(conexao, laco)  # falha aqui é do RF03 e sobe para a main

        if desconectar_apos is not None:
            await asyncio.sleep(desconectar_apos)
            await conexao.desconectar()
            _verificar("desconexão intencional não disparou reconexão", await _aguardar(laco.done, timeout=5))
            return

        try:
            await laco
        except FalhaConexao as e:
            _verificar(f"tentativas esgotadas: falha informada ({e})", True)
            _verificar(f"estado após esgotar é desconectado (atual: {conexao.estado.value})",
                       conexao.estado is EstadoConexao.DESCONECTADO)
    finally:
        await _encerrar(laco)
        await conexao.desconectar()


# ------------------------------------------------------------------ broker

async def teste_rf11(manual: bool = False) -> None:
    """RF11 - Conectar ao broker, sem BLE (a sessão MQTT não depende da pulseira).

    Automático: um eco publicado e recebido pela própria sessão prova a assinatura; depois o TCP é
    cortado sem aviso e o eco tem de voltar sozinho (reconexão + reassinatura).
    manual=True: continua rodando para `docker stop cnpem_hivemq` / `docker start cnpem_hivemq`;
    deve aparecer broker-indisponivel a cada BROKER_INTERVALO_RECONEXAO sem o processo encerrar.
    """
    cliente_id = identificador_coletor()
    topico_eco = f"teste/{cliente_id}/eco"  # fora da hierarquia do RF15, só para o teste
    ecos: list[Mensagem] = []
    conexao = ConexaoBroker(cliente_id=cliente_id, ao_receber=lambda _topico, mensagem: ecos.append(mensagem))
    await conexao.assinar(topico_eco)
    logging.info(f"Broker {conexao.endereco} | client id {cliente_id} | usuário {conexao.usuario or '(sem credenciais)'}")

    async def publicar_ecos() -> None:
        numero = 0
        while True:
            await asyncio.sleep(2)
            if not conexao.conectado:
                continue
            numero += 1
            try:
                await conexao.publicar(
                    topico_eco,
                    Mensagem(tipo=TipoMensagem.ESTADO_PULSEIRA, origem=cliente_id, conteudo={"eco": numero}),
                )
            except BrokerIndisponivel as e:
                logging.warning(f"Eco {numero} não publicado: {e}")

    tarefas = [asyncio.create_task(conexao.manter_conexao()), asyncio.create_task(publicar_ecos())]
    try:
        _verificar("conectou ao broker", await _aguardar(lambda: conexao.conectado, timeout=15))
        _verificar("assinatura funcionando (eco recebido)", await _aguardar(lambda: bool(ecos), timeout=6))

        _derrubar_sem_aviso(conexao)
        await _aguardar(lambda: not conexao.conectado, timeout=5)
        _verificar("reconectou sozinho",
                   await _aguardar(lambda: conexao.conectado, timeout=conexao.intervalo_reconexao + 15))
        antes = len(ecos)
        _verificar("refez a assinatura (eco voltou)", await _aguardar(lambda: len(ecos) > antes, timeout=6))

        if manual:
            logging.info("Modo manual: pare e suba o broker; Ctrl+C para sair")
            await asyncio.Event().wait()
    finally:
        await _encerrar(*tarefas)


async def teste_rf12(simular_queda: bool = True) -> None:
    """RF12 - Presença do coletor, conferida por um espião em coletores/{coletor}/presenca.

    Cobre: online retido ao conectar, sinal de vida, pulseira atendida, last will na queda (simulada
    cortando o TCP), offline no encerramento e nada publicado depois dele.
    """
    cliente_id = identificador_coletor()
    recebidas: list[Mensagem] = []
    espiao = await _espiao(Topicos.presenca_coletor(cliente_id), recebidas=recebidas)
    conexao = ConexaoBroker(cliente_id=cliente_id)
    presenca = PresencaColetor(conexao)  # antes de conectar: registra a última vontade
    intervalo = presenca.intervalo

    def ultimo(campo: str) -> str | None:
        return recebidas[-1].conteudo.get(campo) if recebidas else None

    tarefas = [asyncio.create_task(espiao.manter_conexao())]
    try:
        await _aguardar(lambda: espiao.conectado, timeout=15)
        await asyncio.sleep(1)
        if recebidas:
            logging.info(f"Presença retida de uma execução anterior: {ultimo('estado')}")
        recebidas.clear()

        tarefas += [
            asyncio.create_task(conexao.manter_conexao()),
            asyncio.create_task(presenca.manter_sinal_de_vida()),
        ]
        _verificar("online ao conectar",
                   await _aguardar(lambda: ultimo("estado") == EstadoPresenca.ONLINE.value, timeout=15))
        antes = len(recebidas)
        await asyncio.sleep(intervalo * 2 + 1)
        _verificar(f"sinal de vida a cada {intervalo:g}s", len(recebidas) - antes >= 2)
        mensagem = recebidas[-1]
        _verificar("mensagem tem coletor, estado, pulseira e timestamp",
                   mensagem.origem == cliente_id and {"estado", "pulseira"} <= mensagem.conteudo.keys())

        await presenca.atualiar_pulseira(PULSEIRA_FICTICIA)
        _verificar("troca de pulseira publicada na hora",
                   await _aguardar(lambda: ultimo("pulseira") == PULSEIRA_FICTICIA, timeout=2))

        if simular_queda:
            _derrubar_sem_aviso(conexao)
            _verificar("last will offline publicada na queda",
                       await _aguardar(lambda: ultimo("estado") == EstadoPresenca.OFFLINE.value, timeout=5))
            _verificar("online de novo ao reconectar",
                       await _aguardar(lambda: ultimo("estado") == EstadoPresenca.ONLINE.value,
                                       timeout=conexao.intervalo_reconexao + 15))

        await presenca.anunciar_offline()
        _verificar("offline publicado no encerramento",
                   await _aguardar(lambda: ultimo("estado") == EstadoPresenca.OFFLINE.value, timeout=2))
        antes = len(recebidas)
        await asyncio.sleep(intervalo * 2 + 1)
        _verificar("nada publicado depois do offline", len(recebidas) == antes)
    finally:
        await _encerrar(*tarefas)


async def teste_rf15() -> None:
    """RF15 - Formato, tópicos e QoS padronizados. Não usa BLE nem broker."""
    tabela = {  # tipo: (QoS, retida), conforme a tabela do RF15
        TipoMensagem.PRESENCA:            (1, True),
        TipoMensagem.ESTADO_PULSEIRA:     (0, False),
        TipoMensagem.LIBERACAO_POSSE:     (1, False),
        TipoMensagem.FREQUENCIA_CARDIACA: (0, False),
        TipoMensagem.POSSE:               (1, True),
        TipoMensagem.HANDOFF:             (1, False),
    }
    for tipo, (qos, retida) in tabela.items():
        config = CONFIG_POR_TIPO.get(tipo)
        _verificar(f"{tipo.value}: QoS {qos}, retida={retida}",
                   config is not None and (config.qos, config.retida) == (qos, retida))

    original = Mensagem(
        tipo=TipoMensagem.PRESENCA,
        origem="coletor-teste",
        conteudo={"estado": EstadoPresenca.ONLINE.value, "pulseira": None},
    )
    dados = json.loads(original.para_json())
    _verificar("JSON com versao, tipo, origem, timestamp e conteudo",
               {"versao", "tipo", "origem", "timestamp", "conteudo"} <= dados.keys())
    _verificar("timestamp em UTC", datetime.fromisoformat(dados["timestamp"]).utcoffset() == timedelta(0))
    _verificar("ida e volta preserva a mensagem", Mensagem.de_json(original.para_json()) == original)

    logging.info("Os 3 avisos 'Mensagem descartada' a seguir são esperados")
    _verificar("versão incompatível é descartada",
               Mensagem.de_json(json.dumps({**dados, "versao": VERSAO_FORMATO + 1})) is None)
    _verificar("JSON inválido é descartado", Mensagem.de_json("{não é json") is None)
    _verificar("campo obrigatório ausente é descartado", Mensagem.de_json(json.dumps({"versao": VERSAO_FORMATO})) is None)

    hierarquia = [
        (Topicos.presenca_coletor("c1"),      "coletores/c1/presenca"),
        (Topicos.estado_rssi("c1", "AA"),     "coletores/c1/pulseiras/AA"),
        (Topicos.liberacao_posse("AA"),       "pulseiras/AA/liberacao"),
        (Topicos.frequencia_cardiaca("AA"),   "pulseiras/AA/frequencia-cardiaca"),
        (Topicos.estado_posse("AA"),          "pulseiras/AA/posse"),
        (Topicos.handoff("AA"),               "pulseiras/AA/handoff"),
        (Topicos.presenca_coordenador(),      "coordenador/presenca"),
    ]
    for gerado, documentado in hierarquia:
        _verificar(f"tópico {documentado}", gerado == documentado)
    _verificar("extrai o coletor da presença", Topicos.extrair_presenca_coletor("coletores/c1/presenca") == "c1")
    _verificar("extrai coletor e pulseira do estado/RSSI",
               Topicos.extrair_estado_rssi("coletores/c1/pulseiras/AA") == ("c1", "AA"))
    _verificar("extrai a pulseira da posse", Topicos.extrair_estado_posse("pulseiras/AA/posse") == "AA")
    _verificar("tópico de outro tipo não é extraído", Topicos.extrair_estado_rssi("coletores/c1/presenca") is None)
    _verificar("estados na linguagem ubíqua",
               {e.value for e in EstadoConexao} == {"conectado", "desconectado", "conexao-fraca", "nao-encontrado"})


# ------------------------------------------------------------------ BLE + broker

async def teste_rf09(duracao: float = 60) -> None:
    """RF09 - Publicar estado e RSSI no broker, conferido por um espião em coletores/{coletor}/pulseiras/+.

    Durante o teste, `docker stop cnpem_hivemq`: deve aparecer broker-indisponivel e o RSSI continuar
    no log (o BLE não para); `docker start cnpem_hivemq`: reconecta e republica o último estado.
    No fim, desconecta a pulseira: o estado desconectado também deve ser publicado (RF10).
    """
    # Import local: o módulo usa imports gateway.* e, se quebrar, não deve derrubar os outros testes
    from mqtt.estado_rssi import EstadoRssiColetor

    cliente_id = identificador_coletor()
    endereco = _config("TARGET_ADDRESS")
    recebidas: list[Mensagem] = []
    espiao = await _espiao(Topicos.estado_rssi(cliente_id, "+"), recebidas=recebidas)
    conexao_broker = ConexaoBroker(cliente_id=cliente_id)
    estado_rssi = EstadoRssiColetor(conexao_broker)
    pulseira = ConexaoPulseira(endereco)

    async def ligar_pulseira_ao_broker() -> None:
        """Ponte que o código ainda não tem: cada RSSI lido vira registrar(); a queda vira um estado sem RSSI."""
        intervalo = float(os.getenv("RSSI_INTERVALO", "1"))
        while pulseira.conectado:
            try:
                estado_rssi.registrar(await pulseira.ler_rssi())
            except RSSIIndisponivel as e:
                logging.warning(f"Erro: {e.codigo} | {e}")
            await asyncio.sleep(intervalo)
        estado_rssi.registrar(LeituraRSSI(endereco, pulseira.estado))

    tarefas = [
        asyncio.create_task(espiao.manter_conexao()),
        asyncio.create_task(conexao_broker.manter_conexao()),
        asyncio.create_task(estado_rssi.manter_publicacao()),
    ]
    try:
        await pulseira.conectar()
        ponte = asyncio.create_task(ligar_pulseira_ao_broker())
        tarefas.append(ponte)
        await asyncio.sleep(duracao)
        await pulseira.desconectar()
        await _aguardar(ponte.done, timeout=5)
        await asyncio.sleep(1)  # tempo para o espião receber o desconectado
    finally:
        await pulseira.desconectar()
        await _encerrar(*tarefas)

    if _verificar("espião recebeu estado/RSSI", bool(recebidas)):
        primeira = recebidas[0]
        _verificar("mensagem tem coletor, pulseira, RSSI, estado e timestamp",
                   primeira.origem == cliente_id and {"pulseira", "rssi", "estado"} <= primeira.conteudo.keys())
        _verificar("desconectado publicado ao desconectar",
                   recebidas[-1].conteudo.get("estado") == EstadoConexao.DESCONECTADO.value)


# ------------------------------------------------------------------ main

async def main() -> None:
    """Descomente UM teste por vez."""
    try:
        devices = await find_all_local_devices()
        print(devices)
        # await teste_rf15()                  # mensagens padronizadas: sem BLE e sem broker
        # await teste_rf11()                # broker, sem BLE (manual=True para docker stop/start)
        # await teste_rf12()                # presença, sinal de vida e last will
        # await teste_rf03()                # conectar à pulseira
        # await teste_rf07()                # serviços GATT
        # await teste_rf04()                # RSSI da conexão ativa
        # await teste_rf16()                # frequência cardíaca
        # await teste_rf08()                # estados (esperar_queda=True para uma queda real)
        # await teste_rf10()                # desconexão controlada
        # await teste_rf05()                # reconexão: provoque quedas (desconectar_apos=15 testa a intencional)
        # await teste_rf09()                # BLE + broker
        # await espiar("#")                 # num segundo terminal: tudo o que passa no broker
    except GatewayError as e:
        logging.error(f"Erro: {e.codigo} | {e}")
        _falhas.append(f"teste interrompido por {e.codigo}")
    finally:
        if _falhas:
            logging.error(f"Resumo: {len(_falhas)} falha(s): " + " | ".join(_falhas))
        else:
            logging.info("Resumo: nenhum critério com falha")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
