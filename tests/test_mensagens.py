"""Testes das mensagens padronizadas entre coletores e coordenador (RF15), sem hardware e sem broker.

Rodar na raiz do projeto:  python tests/test_mensagens.py
(também funciona com pytest)
"""
import asyncio
import json
import logging
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.domain.models import EstadoConexao, LeituraRSSI  # noqa: E402
from coletor.coletor import Coletor  # noqa: E402
from coordenador.coordenador import Coordenador  # noqa: E402
from mqtt.domain.models import (  # noqa: E402
    CAMPOS_POR_TIPO, CONFIG_POR_TIPO, VERSAO_FORMATO, EstadoPresenca, Mensagem, TipoMensagem,
)
from mqtt.domain.topicos import Topicos  # noqa: E402
from mqtt.estado_rssi import EstadoRssiColetor  # noqa: E402
from mqtt.presenca import PresencaColetor  # noqa: E402

PULSEIRA = "A0:9E:1A:00:00:01"
COLETOR = "coletor-a"
COORDENADOR = "coordenador"


class BrokerFalso:
    """Dublê da ConexaoBroker: guarda cada publicação e confere que ela passa no de_json,
    ou seja, que quem recebe consegue interpretar tudo o que o código publica."""

    def __init__(self, cliente_id: str = COLETOR) -> None:
        self.cliente_id = cliente_id
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.publicadas: list[tuple[str, Mensagem]] = []

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    def adicionar_ao_desconectar(self, _tratador: Callable) -> None:
        pass

    async def assinar(self, _topico: str, qos: int = 1) -> None:
        pass

    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        assert Mensagem.de_json(mensagem.para_json(), topico) is not None, f"{topico}: {mensagem}"
        self.publicadas.append((topico, mensagem))
        await asyncio.sleep(0)

    def do_tipo(self, tipo: TipoMensagem) -> list[tuple[str, Mensagem]]:
        return [(topico, mensagem) for topico, mensagem in self.publicadas if mensagem.tipo is tipo]


class CapturaLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.mensagens: list[str] = []

    def emit(self, registro: logging.LogRecord) -> None:
        self.mensagens.append(registro.getMessage())


@contextmanager
def capturar_log() -> Iterator[list[str]]:
    raiz = logging.getLogger()
    captura, nivel = CapturaLog(), raiz.level
    raiz.addHandler(captura)
    raiz.setLevel(logging.DEBUG)
    try:
        yield captura.mensagens
    finally:
        raiz.removeHandler(captura)
        raiz.setLevel(nivel)


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


async def esperar_para_sempre() -> None:
    await asyncio.sleep(3600)


def envelope(**alteracoes: object) -> str:
    """JSON de uma posse válida, com os campos do envelope trocados ou removidos (valor ...)."""
    dados = {
        "versao": VERSAO_FORMATO, "tipo": "posse", "origem": COORDENADOR,
        "timestamp": "2026-11-10T12:00:00+00:00", "conteudo": {"pulseira": PULSEIRA, "coletor": None},
    }
    dados.update(alteracoes)
    return json.dumps({chave: valor for chave, valor in dados.items() if valor is not ...})


# ------------------------------------------------------------------ envelope e versão

def test_envelope_tem_os_cinco_campos_e_timestamp_utc():
    mensagem = Mensagem(TipoMensagem.POSSE, COORDENADOR, {"pulseira": PULSEIRA, "coletor": COLETOR})
    dados = json.loads(mensagem.para_json())

    assert dados.keys() == {"versao", "tipo", "origem", "timestamp", "conteudo"}
    assert dados["versao"] == VERSAO_FORMATO and dados["tipo"] == "posse" and dados["origem"] == COORDENADOR
    assert mensagem.timestamp.utcoffset() == timedelta(0)
    assert dados["timestamp"].endswith("+00:00")
    assert Mensagem.de_json(mensagem.para_json()) == mensagem


def test_timestamp_utc_com_z_e_aceito():
    assert Mensagem.de_json(envelope(timestamp="2026-11-10T12:00:00Z")) is not None


def test_versao_diferente_e_descartada_e_registrada_em_log():
    with capturar_log() as log:
        resultado = Mensagem.de_json(envelope(versao=VERSAO_FORMATO - 1, origem="coletor-velho"), "pulseiras/X/posse")

    assert resultado is None
    assert any(
        "Tópico: pulseiras/X/posse" in linha and "Origem: coletor-velho" in linha
        and f"versão {VERSAO_FORMATO - 1}" in linha
        for linha in log
    ), log


def test_formato_invalido_e_descartado_sem_levantar_erro():
    casos = {
        "JSON inválido": "{não é json",
        "payload não é objeto": "[1, 2]",
        "versao ausente": envelope(versao=...),
        "versao booleana": envelope(versao=True),
        "versao texto": envelope(versao=str(VERSAO_FORMATO)),
        "versao real": envelope(versao=float(VERSAO_FORMATO)),
        "tipo desconhecido": envelope(tipo="pedido-de-posse"),
        "origem ausente": envelope(origem=...),
        "origem numérica": envelope(origem=42),
        "origem vazia": envelope(origem=""),
        "conteudo lista": envelope(conteudo=[1, 2]),
        "conteudo texto": envelope(conteudo="x"),
        "conteudo nulo": envelope(conteudo=None),
        "timestamp sem fuso": envelope(timestamp="2026-11-10T12:00:00"),
        "timestamp fora de UTC": envelope(timestamp="2026-11-10T09:00:00-03:00"),
        "timestamp numérico": envelope(timestamp=1731240000),
        "posse sem pulseira": envelope(conteudo={"coletor": None}),
        "handoff sem id": envelope(tipo="handoff", conteudo={"pulseira": PULSEIRA, "origem": COLETOR}),
    }
    with capturar_log():
        aceitos = [nome for nome, payload in casos.items() if Mensagem.de_json(payload) is not None]
    assert not aceitos, aceitos


# ------------------------------------------------------------------ tópicos, QoS e conteúdo por tipo

def test_hierarquia_de_topicos_do_rf15():
    assert Topicos.presenca_coletor("c1") == "coletores/c1/presenca"
    assert Topicos.sinal_de_vida_coletor("c1") == "coletores/c1/sinal-de-vida"
    assert Topicos.estado_rssi("c1", "AA") == "coletores/c1/pulseiras/AA"
    assert Topicos.liberacao_posse("AA") == "pulseiras/AA/liberacao"
    assert Topicos.frequencia_cardiaca("AA") == "pulseiras/AA/frequencia-cardiaca"
    assert Topicos.estado_posse("AA") == "pulseiras/AA/posse"
    assert Topicos.handoff("AA") == "pulseiras/AA/handoff"
    assert Topicos.presenca_coordenador() == "coordenador/presenca"
    assert Topicos.sinal_de_vida_coordenador() == "coordenador/sinal-de-vida"


def test_extrair_devolve_os_ids_so_do_proprio_topico():
    assert Topicos.extrair_presenca_coletor("coletores/c1/presenca") == "c1"
    assert Topicos.extrair_estado_rssi("coletores/c1/pulseiras/AA") == ("c1", "AA")
    assert Topicos.extrair_liberacao_posse("pulseiras/AA/liberacao") == "AA"
    assert Topicos.extrair_estado_posse("pulseiras/AA/posse") == "AA"
    assert Topicos.extrair_handoff("pulseiras/AA/handoff") == "AA"

    assert Topicos.extrair_presenca_coletor("coletores/c1/sinal-de-vida") is None
    assert Topicos.extrair_estado_rssi("coletores/c1/presenca") is None
    assert Topicos.extrair_handoff("pulseiras/AA/posse") is None
    assert Topicos.extrair_estado_posse("pulseiras/AA/handoff") is None


def test_qos_e_retencao_por_tipo():
    esperado = {  # tipo: (QoS, retida), conforme a tabela do RF15
        TipoMensagem.PRESENCA:            (1, True),
        TipoMensagem.SINAL_DE_VIDA:       (0, False),
        TipoMensagem.ESTADO_PULSEIRA:     (0, False),
        TipoMensagem.LIBERACAO_POSSE:     (1, False),
        TipoMensagem.FREQUENCIA_CARDIACA: (0, False),
        TipoMensagem.POSSE:               (1, True),
        TipoMensagem.HANDOFF:             (1, False),
    }
    assert {tipo: (c.qos, c.retida) for tipo, c in CONFIG_POR_TIPO.items()} == esperado
    assert set(TipoMensagem) == esperado.keys()


def test_campos_obrigatorios_por_tipo():
    esperado = {  # campos sempre presentes, conforme a tabela de conteúdo do RF15
        TipoMensagem.PRESENCA:            {"estado"},
        TipoMensagem.SINAL_DE_VIDA:       set(),
        TipoMensagem.ESTADO_PULSEIRA:     {"pulseira", "estado", "rssi"},
        TipoMensagem.LIBERACAO_POSSE:     {"pulseira"},
        TipoMensagem.FREQUENCIA_CARDIACA: {"pulseira", "coletor", "bpm"},
        TipoMensagem.POSSE:               {"pulseira", "coletor"},
        TipoMensagem.HANDOFF:             {"id", "pulseira", "origem"},
    }
    assert CAMPOS_POR_TIPO == esperado


def test_estado_pulseira_so_tem_amostras_na_varredura():
    estado_rssi = EstadoRssiColetor(BrokerFalso())
    varredura = estado_rssi.mensagem(LeituraRSSI(PULSEIRA, EstadoConexao.DESCONECTADO, -60, 7))
    conexao = estado_rssi.mensagem(LeituraRSSI(PULSEIRA, EstadoConexao.CONECTADO, -70))
    sem_rssi = estado_rssi.mensagem(LeituraRSSI(PULSEIRA, EstadoConexao.NAO_ENCONTRADO))

    assert varredura.conteudo == {"pulseira": PULSEIRA, "estado": "desconectado", "rssi": -60, "amostras": 7}
    assert conexao.conteudo == {"pulseira": PULSEIRA, "estado": "conectado", "rssi": -70}
    assert sem_rssi.conteudo == {"pulseira": PULSEIRA, "estado": "nao-encontrado", "rssi": None}
    for mensagem in (varredura, conexao, sem_rssi):
        assert Mensagem.de_json(mensagem.para_json()) is not None


# ------------------------------------------------------------------ presença e sinal de vida

def test_sinal_de_vida_do_coletor_vai_para_topico_proprio_sem_campos():
    async def cenario():
        broker = BrokerFalso()
        presenca = PresencaColetor(broker, intervalo=0.01)
        tarefa = asyncio.create_task(presenca.manter_sinal_de_vida())
        try:
            await aguardar(lambda: len(broker.publicadas) >= 2)
        finally:
            tarefa.cancel()
            await asyncio.gather(tarefa, return_exceptions=True)

        assert broker.publicadas, "nenhum sinal de vida publicado"
        for topico, mensagem in broker.publicadas:
            assert topico == Topicos.sinal_de_vida_coletor(COLETOR)
            assert mensagem.tipo is TipoMensagem.SINAL_DE_VIDA and mensagem.conteudo == {}

    asyncio.run(cenario())


def test_presenca_offline_nao_atende_pulseira():
    async def cenario():
        broker = BrokerFalso()
        presenca = PresencaColetor(broker)
        await presenca.atualizar_pulseira(PULSEIRA)

        online = presenca.mensagem(EstadoPresenca.ONLINE)
        offline = presenca.mensagem(EstadoPresenca.OFFLINE)
        ultima_vontade = Mensagem.de_json(broker.ultima_vontade.payload)

        assert online.conteudo == {"estado": "online", "pulseira": PULSEIRA}
        assert offline.conteudo == {"estado": "offline", "pulseira": None}
        assert ultima_vontade.conteudo == {"estado": "offline", "pulseira": None}

    asyncio.run(cenario())


def test_presenca_do_coletor_acompanha_a_posse():
    async def cenario():
        broker = BrokerFalso()
        coletor = Coletor(broker, PULSEIRA)
        coletor.varredura.executar = esperar_para_sempre
        coletor._manter_sessao = esperar_para_sempre
        try:
            posse = Mensagem(TipoMensagem.POSSE, COORDENADOR, {"pulseira": PULSEIRA, "coletor": COLETOR})
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse)
            presencas = broker.do_tipo(TipoMensagem.PRESENCA)
            assert presencas and presencas[-1][1].conteudo == {"estado": "online", "pulseira": PULSEIRA}

            liberada = Mensagem(TipoMensagem.POSSE, COORDENADOR, {"pulseira": PULSEIRA, "coletor": None})
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), liberada)
            assert broker.do_tipo(TipoMensagem.PRESENCA)[-1][1].conteudo == {"estado": "online", "pulseira": None}
        finally:
            await coletor._parar_varredura()
            if coletor._sessao is not None:
                coletor._sessao.cancel()
                await asyncio.gather(coletor._sessao, return_exceptions=True)

    asyncio.run(cenario())


def test_sinal_de_vida_do_coordenador_vai_para_topico_proprio():
    async def cenario():
        broker = BrokerFalso(COORDENADOR)
        coordenador = Coordenador(broker, intervalo=0.01, pulseiras=[PULSEIRA])
        tarefa = asyncio.create_task(coordenador.manter_sinal_de_vida())
        try:
            await aguardar(lambda: len(broker.publicadas) >= 2)
        finally:
            tarefa.cancel()
            await asyncio.gather(tarefa, return_exceptions=True)

        assert {topico for topico, _ in broker.publicadas} == {Topicos.sinal_de_vida_coordenador()}
        assert all(m.tipo is TipoMensagem.SINAL_DE_VIDA and m.conteudo == {} for _, m in broker.publicadas)
        assert coordenador.mensagem(EstadoPresenca.ONLINE).conteudo == {"estado": "online"}  # sem pulseira

    asyncio.run(cenario())


# ------------------------------------------------------------------ handoff e liberação

def test_liberacao_do_coletor_responde_ao_handoff_com_o_id():
    async def cenario():
        broker = BrokerFalso()
        coletor = Coletor(broker, PULSEIRA)
        coletor.varredura.executar = esperar_para_sempre
        coletor._manter_sessao = esperar_para_sempre
        try:
            posse = Mensagem(TipoMensagem.POSSE, COORDENADOR, {"pulseira": PULSEIRA, "coletor": COLETOR})
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse)

            # Tópico de outra pulseira: o coletor confia no tópico, não no conteúdo
            alheio = Mensagem(TipoMensagem.HANDOFF, COORDENADOR, {"id": "x", "pulseira": PULSEIRA, "origem": COLETOR})
            await coletor.ao_receber(Topicos.handoff("OUTRA"), alheio)
            assert not broker.do_tipo(TipoMensagem.LIBERACAO_POSSE)

            handoff = Mensagem(TipoMensagem.HANDOFF, COORDENADOR, {"id": "ab12cd34", "pulseira": PULSEIRA, "origem": COLETOR})
            await coletor.ao_receber(Topicos.handoff(PULSEIRA), handoff)
            topico, liberacao = broker.do_tipo(TipoMensagem.LIBERACAO_POSSE)[-1]
            assert topico == Topicos.liberacao_posse(PULSEIRA)
            assert liberacao.conteudo == {"pulseira": PULSEIRA, "handoff": "ab12cd34"}

            await coletor._liberar("encerramento")  # fora do handoff, sem o campo
            assert broker.do_tipo(TipoMensagem.LIBERACAO_POSSE)[-1][1].conteudo == {"pulseira": PULSEIRA}
        finally:
            await coletor._parar_varredura()

    asyncio.run(cenario())


def test_handoff_do_coordenador_tem_id_que_aparece_no_log_ate_a_disputa():
    async def cenario():
        broker = BrokerFalso(COORDENADOR)
        coordenador = Coordenador(broker, pulseiras=[PULSEIRA])
        coordenador.janela_disputa = 3600  # a disputa não termina durante o teste
        coordenador.visao.atualizar_posse(PULSEIRA, COLETOR)
        coordenador._leituras_fracas[PULSEIRA] = coordenador.minimo_fracas
        try:
            with capturar_log() as log:
                await coordenador._iniciar_handoff(PULSEIRA, COLETOR)
                topico, comando = broker.do_tipo(TipoMensagem.HANDOFF)[-1]
                id_handoff = comando.conteudo["id"]

                liberacao = Mensagem(TipoMensagem.LIBERACAO_POSSE, COLETOR, {"pulseira": PULSEIRA, "handoff": id_handoff})
                await coordenador.ao_receber(Topicos.liberacao_posse(PULSEIRA), liberacao)
                await aguardar(lambda: bool(broker.do_tipo(TipoMensagem.POSSE)))
                await aguardar(lambda: any("Disputa aberta" in linha for linha in log))

            assert topico == Topicos.handoff(PULSEIRA)
            assert comando.conteudo == {"id": id_handoff, "pulseira": PULSEIRA, "origem": COLETOR}
            assert broker.do_tipo(TipoMensagem.POSSE)[-1][1].conteudo == {"pulseira": PULSEIRA, "coletor": None}
            com_id = [linha for linha in log if id_handoff in linha]
            for evento in ("Handoff iniciado", "Liberação recebida", "Disputa aberta"):
                assert any(evento in linha for linha in com_id), (evento, log)
        finally:
            tarefas = list(coordenador._disputas.values())
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)

    asyncio.run(cenario())


def test_coordenador_descarta_conexao_fraca_publicada_por_coletor():
    async def cenario():
        coordenador = Coordenador(BrokerFalso(COORDENADOR), pulseiras=[PULSEIRA])

        def sinal(estado: str) -> Mensagem:
            return Mensagem(TipoMensagem.ESTADO_PULSEIRA, COLETOR, {"pulseira": PULSEIRA, "estado": estado, "rssi": -80})

        with capturar_log():
            await coordenador.ao_receber(Topicos.estado_rssi(COLETOR, PULSEIRA), sinal("conexao-fraca"))
        assert COLETOR not in coordenador.visao.sinais.get(PULSEIRA, {})

        await coordenador.ao_receber(Topicos.estado_rssi(COLETOR, PULSEIRA), sinal("conectado"))
        assert COLETOR in coordenador.visao.sinais[PULSEIRA]

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
