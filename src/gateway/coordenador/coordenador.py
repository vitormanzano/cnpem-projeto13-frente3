import asyncio
import contextlib
import os
import logging
import time
import uuid
from dataclasses import dataclass, field

import aiomqtt

from bluetooth.domain.erros import BrokerIndisponivel
from bluetooth.domain.models import EstadoConexao
from bluetooth.scanner import ler_endereco_pulseira
from coletor.coletor import ler_prazo_conexao
from coordenador.decisao import MARGEM_HISTERESE_PADRAO, escolher_coletor
from coordenador.visao import MINIMO_AMOSTRAS_PADRAO, SinalColetor, VisaoColetores
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import EstadoPresenca, Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos


INTERVALO_SINAL_DE_VIDA_PADRAO = 5.0
JANELA_DISPUTA_PADRAO          = 3.0 # A mesma na disputa e no handoff (RF14)
FATOR_EXTENSAO_JANELA_PADRAO   = 3.0 # Sem sinal válido, a janela vai até janela * fator
INTERVALO_VERFICACAO           = 0.5 # s entre verificações durante a extensão
LIMIAR_CONEXAO_FRACA_PADRAO    = -78
LEITURAS_FRACAS_PADRAO         = 3
PRAZO_LIBERACAO_PADRAO         = 3.0 # s para a origem confirmar que soltou
PRAZO_SINAL_DE_VIDA_PADRAO     = 15.0 # s sem mensagem até o coletor ficar indisponível (RF13)
INTERVALO_VERIFICACAO_COLETORES = 1.0 # s entre as verificações de sinal de vida e posse órfã


def ler_pulseiras_acompanhadas(pulseiras: list[str] | None = None) -> frozenset[str]:
    """Resolve as pulseiras que o coordenador acompanha (RF13, RF19).

    Args:
        pulseiras (list[str] | None): Endereços MAC. Se omitido, são lidos de PULSEIRAS (separados
            por vírgula) ou, sem ela, de TARGET_ADDRESS (na versão atual há uma só pulseira).

    Returns:
        frozenset[str]: Endereços em maiúsculas.

    Raises:
        ValueError: Se nenhuma pulseira estiver configurada ou algum endereço não for um MAC válido.
    """
    if pulseiras is None:
        valor = os.getenv("PULSEIRAS") or os.getenv("TARGET_ADDRESS") or ""
        pulseiras = [pulseira for pulseira in valor.split(",") if pulseira.strip()]

    # Sem pulseiras o coordenador ignoraria todas as mensagens: melhor falhar na inicialização
    if not pulseiras:
        raise ValueError("O coordenador precisa conhecer as pulseiras que acompanha. Defina PULSEIRAS (ou TARGET_ADDRESS).")

    enderecos: set[str] = set()
    for pulseira in pulseiras:
        try:
            enderecos.add(ler_endereco_pulseira(pulseira))
        except ValueError as e:
            raise ValueError(
                f"Endereço de pulseira inválido: {pulseira!r} (PULSEIRAS ou TARGET_ADDRESS). "
                "Use endereços MAC no formato AA:BB:CC:DD:EE:FF, separados por vírgula."
            ) from e
    return frozenset(enderecos)


def _numero_do_ambiente(variavel: str, padrao: float, tipo: type[int] | type[float] = float) -> int | float:
    """Lê um número da variável de ambiente; sem valor, usa o padrão.

    Raises:
        ValueError: Se o valor não for um número do tipo pedido.
    """
    valor = os.getenv(variavel) or str(padrao)
    try:
        return tipo(valor)
    except ValueError as e:
        esperado = "um inteiro" if tipo is int else "um número"
        raise ValueError(f"{variavel} deve ser {esperado}, recebido {valor!r}.") from e


def ler_janela_disputa(janela: float | None = None) -> float:
    """Resolve a janela em que o coordenador reúne os sinais dos coletores, a mesma na disputa e
    no handoff (RF14).

    Args:
        janela (float | None): Segundos. Se omitido, é lido de JANELA_DISPUTA (padrão de 3 s).

    Raises:
        ValueError: Se não for um número maior que zero.
    """
    if janela is None:
        janela = _numero_do_ambiente("JANELA_DISPUTA", JANELA_DISPUTA_PADRAO)
    if janela <= 0:
        raise ValueError("JANELA_DISPUTA deve ser maior que zero.")
    return janela


def ler_fator_extensao(fator: float | None = None) -> float:
    """Resolve até quantas vezes a janela se estende sem nenhum sinal válido (RF14).

    Args:
        fator (float | None): Múltiplo da janela. Se omitido, é lido de FATOR_EXTENSAO_JANELA (padrão de 3).

    Raises:
        ValueError: Se não for um número maior ou igual a 1.
    """
    if fator is None:
        fator = _numero_do_ambiente("FATOR_EXTENSAO_JANELA", FATOR_EXTENSAO_JANELA_PADRAO)
    # Menor que 1, a janela estendida acabaria antes da própria janela
    if fator < 1:
        raise ValueError("FATOR_EXTENSAO_JANELA deve ser maior ou igual a 1.")
    return fator


def ler_margem_histerese(margem: float | None = None) -> float:
    """Resolve a vantagem que um candidato precisa ter sobre o favorito (RF14).

    Args:
        margem (float | None): dB. Se omitido, é lido de MARGEM_HISTERESE (padrão de 5 dB).

    Raises:
        ValueError: Se for negativa.
    """
    if margem is None:
        margem = _numero_do_ambiente("MARGEM_HISTERESE", MARGEM_HISTERESE_PADRAO)
    if margem < 0:
        raise ValueError("MARGEM_HISTERESE não pode ser negativa.")
    return margem


def ler_minimo_amostras(minimo: int | None = None) -> int:
    """Resolve quantos anúncios uma mediana precisa ter para o sinal ser válido (RF14).

    Args:
        minimo (int | None): Se omitido, é lido de MINIMO_AMOSTRAS (padrão de 5).

    Raises:
        ValueError: Se não for um inteiro maior ou igual a 1.
    """
    if minimo is None:
        minimo = _numero_do_ambiente("MINIMO_AMOSTRAS", MINIMO_AMOSTRAS_PADRAO, int)
    if minimo < 1:
        raise ValueError("MINIMO_AMOSTRAS deve ser pelo menos 1.")
    return minimo


def ler_prazo_sinal_de_vida(prazo: float | None = None) -> float:
    """Resolve o tempo sem nenhuma mensagem de um coletor até considerá-lo indisponível (RF13).

    Se omitido, é lido de PRAZO_SINAL_DE_VIDA_COLETOR (padrão de 15 s).

    Raises:
        ValueError: Se o prazo não for um número maior que zero.
    """
    if prazo is None:
        valor = os.getenv("PRAZO_SINAL_DE_VIDA_COLETOR") or str(PRAZO_SINAL_DE_VIDA_PADRAO)
        try:
            prazo = float(valor)
        except ValueError as e:
            raise ValueError(f"PRAZO_SINAL_DE_VIDA_COLETOR deve ser um número, recebido {valor!r}.") from e

    if prazo <= 0:
        raise ValueError("PRAZO_SINAL_DE_VIDA_COLETOR deve ser maior que zero.")
    return prazo


def ler_intervalo_sinal_de_vida(intervalo: float | None = None) -> float:
    """Resolve o intervalo de sinal de vida.

    Se omitido, é lido da variável de ambiente PRESENCA_INTERVALO_COORDENADOR (padrão de 5 s).

    Raises:
        ValueError: Se o intervalo não for positivo ou a variável de ambiente não for um número.
    """
    if intervalo is None:
        intervalo = float(os.getenv("PRESENCA_INTERVALO_COORDENADOR", INTERVALO_SINAL_DE_VIDA_PADRAO))

    if intervalo <= 0:
        raise ValueError("PRESENCA_INTERVALO_COORDENADOR deve ser maior que zero.")
    return intervalo


def ler_limiar_conexao_fraca(limiar: int | None = None) -> int:
    """Resolve o limiar de RSSI abaixo do qual a leitura do coletor ativo conta como fraca (RF19).

    O painel usa a mesma função, para que a faixa crítica coincida com a conexao-fraca (RF20).

    Args:
        limiar (int | None): Limiar em dBm. Se omitido, é lido de LIMIAR_CONEXAO_FRACA (padrão de -78 dBm).

    Raises:
        ValueError: Se não for um inteiro negativo.
    """
    if limiar is None:
        valor = os.getenv("LIMIAR_CONEXAO_FRACA") or str(LIMIAR_CONEXAO_FRACA_PADRAO)
        try:
            limiar = int(valor)
        except ValueError as e:
            raise ValueError(f"LIMIAR_CONEXAO_FRACA deve ser um inteiro em dBm, recebido {valor!r}.") from e

    # RSSI em dBm é sempre negativo: um limiar >= 0 marcaria toda leitura como fraca
    if limiar >= 0:
        raise ValueError(f"LIMIAR_CONEXAO_FRACA deve ser negativo (dBm), recebido {limiar}.")
    return limiar


def _inteiro(valor: object, campo: str, nulo: bool = False) -> int | None:
    """Campo inteiro do conteúdo de uma mensagem (RF15).

    Raises:
        TypeError: Se não for inteiro (ou None, com `nulo`).
    """
    if valor is None and nulo:
        return None
    # type() e não isinstance(): bool é subclasse de int (mesmo cuidado do Mensagem.de_json)
    if type(valor) is not int:
        raise TypeError(f"{campo} {valor!r} não é inteiro")
    return valor


@dataclass
class _Espera:
    """Resposta que o coordenador aguarda de um coletor: a liberação no handoff (RF06) ou o
    estado conectado depois da concessão (RF14)."""
    coletor: str
    fim: asyncio.Event = field(default_factory=asyncio.Event)
    sucesso: bool = False
    em: float = 0.0 # time.monotonic() da resposta


    def responder(self, sucesso: bool) -> None:
        """Vale a primeira resposta; as seguintes são ignoradas."""
        if self.fim.is_set():
            return
        self.sucesso, self.em = sucesso, time.monotonic()
        self.fim.set()


class Coordenador:
    """Responsável por gerenciar as conexões entre coletores e pulseiras."""

    def __init__(
        self,
        broker: "ConexaoBroker",
        intervalo: float | None = None,
        pulseiras: list[str] | None = None,
        prazo_sinal_de_vida: float | None = None,
    ) -> None:
        """Registra sua presença e coordena os coletores.
            Args:
                broker (ConexaoBroker): Sessão MQTT do coletor.
                intervalo (float | None): Segundos entre os sinais de vida. Se omitido, é lido
                    de PRESENCA_INTERVALO (padrão de 5 s).
                pulseiras (list[str] | None): Pulseiras acompanhadas; mensagens de outras são ignoradas
                    (RF13). Se omitido, são lidas de PULSEIRAS ou TARGET_ADDRESS.
                prazo_sinal_de_vida (float | None): Segundos sem mensagem de um coletor até considerá-lo
                    indisponível (RF13). Se omitido, é lido de PRAZO_SINAL_DE_VIDA_COLETOR (padrão de 15 s).

            A disputa pela posse (RF14) é configurada pelo ambiente: JANELA_DISPUTA (padrão de 3 s),
            FATOR_EXTENSAO_JANELA (3), MARGEM_HISTERESE (5 dB), MINIMO_AMOSTRAS (5) e PRAZO_CONEXAO (5 s).

            Raises:
                ValueError: Se o intervalo, as pulseiras, o prazo de sinal de vida ou a configuração
                    da disputa forem inválidos.
        """
        self.broker:           ConexaoBroker            = broker
        self.intervalo:        float                    = ler_intervalo_sinal_de_vida(intervalo)
        self.pulseiras:        frozenset[str]           = ler_pulseiras_acompanhadas(pulseiras)
        self.prazo_sinal_de_vida: float                 = ler_prazo_sinal_de_vida(prazo_sinal_de_vida)
        self.visao                                      = VisaoColetores(minimo_amostras=ler_minimo_amostras())
        self._encerrada                                 = False
        self._ouvindo                                   = False # Conectado, com a contagem de sinal de vida reiniciada
        self._pulseiras_ignoradas: set[str]             = set() # Não configuradas, já avisadas no log

        self.broker.ultima_vontade                      = self._ultima_vontade()
        self.broker.keepalive                           = self.intervalo # Igual ao sinal de vida, como no coletor (RF11)
        self.broker.adicionar_ao_conectar(self.anunciar_online)
        self.broker.adicionar_ao_conectar(self._reiniciar_contagem)
        self.broker.adicionar_ao_desconectar(self._parar_contagem)
        self.broker.ao_receber                          = self.ao_receber

        self.janela_disputa                             = ler_janela_disputa()
        self.fator_extensao                             = ler_fator_extensao()
        self.margem                                     = ler_margem_histerese()
        self.prazo_conexao                              = ler_prazo_conexao()
        self.limiar                                     = ler_limiar_conexao_fraca()
        self.minimo_fracas                              = int(os.getenv("LEITURAS_FRACAS", LEITURAS_FRACAS_PADRAO))
        self.prazo_liberacao                            = float(os.getenv("PRAZO_LIBERACAO", PRAZO_LIBERACAO_PADRAO))

        self._liberacoes:      dict[str, _Espera]       = {} # Pulseira -> handoff aguardando a origem soltar (RF06)
        self._esperas_conexao: dict[str, _Espera]       = {} # Pulseira -> coletor com a posse aguardando conectar (RF14)
        self._disputas:        dict[str, asyncio.Task]  = {} # Pulseira -> disputa ou handoff em andamento
        self._leituras_fracas: dict[str, int]           = {} # Pulseira -> leituras fracas seguidas do dono
        self._ultimo_ativo:    dict[str, str]           = {} # Pulseira -> último coletor conectado (favorito)


    async def assinar_topicos(self) -> None:
        """Assina presença, sinal de vida, estado/RSSI e liberação de todos os coletores, e a posse
        retida (RF13).

        O curinga vale também para as pulseiras: assim as não configuradas chegam e são
        registradas em log antes de ignoradas.

        Pode ser chamado antes de conectar: a ConexaoBroker refaz as assinaturas de cada
        (re)conexão, e o broker entrega na hora as mensagens retidas de presença e posse.
        """
        await self.broker.assinar(Topicos.presenca_coletor("+"))
        await self.broker.assinar(Topicos.sinal_de_vida_coletor("+"), qos=0)
        await self.broker.assinar(Topicos.estado_rssi("+", "+"), qos=0)
        await self.broker.assinar(Topicos.estado_posse("+"))
        await self.broker.assinar(Topicos.liberacao_posse("+"))


    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        """Encaminha cada mensagem pelo tipo; o tópico só é usado para extrair os ids."""
        match mensagem.tipo:
            case TipoMensagem.PRESENCA:
                await self._tratar_presenca(topico, mensagem)
            case TipoMensagem.SINAL_DE_VIDA:
                self._tratar_sinal_de_vida(topico, mensagem)
            case TipoMensagem.ESTADO_PULSEIRA:
                await self._tratar_sinal(topico, mensagem)
            case TipoMensagem.POSSE:
                await self._tratar_posse(topico, mensagem)
            case TipoMensagem.LIBERACAO_POSSE:
                await self._tratar_liberacao(topico, mensagem)


    def _acompanha(self, pulseira: str, topico: str) -> bool:
        """Se a pulseira está configurada (RF13). A não configurada vai para o log em WARNING na
        primeira mensagem e em DEBUG nas seguintes, porque o RSSI dela chega a cada segundo."""
        if pulseira in self.pulseiras:
            return True
        nivel = logging.DEBUG if pulseira in self._pulseiras_ignoradas else logging.WARNING
        self._pulseiras_ignoradas.add(pulseira)
        logging.log(nivel, f"Mensagem ignorada | Tópico: {topico} | Pulseira {pulseira} não configurada (PULSEIRAS)")
        return False


    def _registrar_mensagem(self, coletor: str, topico: str, mensagem: Mensagem, aceitar_antiga: bool = False) -> bool:
        """Confere a ordem e renova o sinal de vida do coletor, considerando todos os tópicos dele (RF13).

        Returns:
            bool: False se a mensagem for mais antiga que a última do coletor e deve ser ignorada.
        """
        estava_sem_sinal = coletor in self.visao.coletores_sem_sinal
        if not self.visao.registrar_mensagem(coletor, mensagem.timestamp, aceitar_antiga=aceitar_antiga):
            # Estado/RSSI e sinal de vida são frequentes e a perda de um é compensada pelo seguinte (RF15)
            frequente = mensagem.tipo in (TipoMensagem.ESTADO_PULSEIRA, TipoMensagem.SINAL_DE_VIDA)
            logging.log(
                logging.DEBUG if frequente else logging.WARNING,
                f"Mensagem antiga ignorada | Tópico: {topico} | Coletor: {coletor} | "
                f"Timestamp: {mensagem.timestamp.isoformat()} | Último: {self.visao.ultimo_timestamp[coletor].isoformat()}"
            )
            return False
        if estava_sem_sinal and self.visao.disponivel(coletor):
            logging.info(f"Coletor disponível | Coletor: {coletor} | Sinal de vida voltou")
        return True


    async def _tratar_presenca(self, topico: str, mensagem: Mensagem) -> None:
        coletor = Topicos.extrair_presenca_coletor(topico)
        if coletor is None:
            return

        try:
            if mensagem.origem != coletor:
                raise ValueError(f"origem {mensagem.origem!r} não é o coletor do tópico")
            estado = EstadoPresenca(mensagem.conteudo["estado"])
            pulseira = mensagem.conteudo.get("pulseira")
            if pulseira is not None and (not isinstance(pulseira, str) or not pulseira):
                raise TypeError(f"pulseira {pulseira!r} não é um endereço")
        except (ValueError, TypeError) as e:
            logging.warning(f"Presença descartada | Tópico: {topico} | Conteúdo inválido [{type(e).__name__}]: {e}")
            return

        online = estado is EstadoPresenca.ONLINE
        # A última vontade leva o timestamp do registro, não o da queda (PresencaColetor._ultima_vontade),
        # então o offline vale mesmo mais antigo que a última mensagem do coletor
        if not self._registrar_mensagem(coletor, topico, mensagem, aceitar_antiga=not online):
            return

        # Só registra mudança: a presença também é republicada a cada reconexão e troca de pulseira
        if self.visao.atualizar_presenca(coletor, online, pulseira if online else None):
            logging.info(f"Presença | Coletor: {coletor} | {estado.value}")

        if not online:
            for pulseira_orfa in self.visao.pulseiras_de(coletor):
                await self._liberar_posse_orfa(pulseira_orfa, coletor)
            return
        self._conferir_pulseira_atendida(coletor, pulseira)


    def _conferir_pulseira_atendida(self, coletor: str, pulseira: str | None) -> None:
        """Na divergência entre a presença e a posse retida, vale a posse (RF13): a presença só fica
        registrada. Se o responsável não estiver online, a verificação periódica libera a posse."""
        com_posse = self.visao.pulseiras_de(coletor)
        if com_posse == ([] if pulseira is None else [pulseira]):
            return
        logging.info(
            f"Presença diverge da posse | Coletor: {coletor} | Presença: {pulseira or 'nenhuma'} | "
            f"Posse: {', '.join(com_posse) or 'nenhuma'} | Vale a posse"
        )


    def _tratar_sinal_de_vida(self, topico: str, mensagem: Mensagem) -> None:
        coletor = Topicos.extrair_sinal_de_vida_coletor(topico)
        if coletor is None:
            return
        if mensagem.origem != coletor:
            logging.warning(f"Sinal de vida descartado | Tópico: {topico} | origem {mensagem.origem!r} não é o coletor do tópico")
            return
        if self._registrar_mensagem(coletor, topico, mensagem):
            logging.debug(f"Sinal de vida | Coletor: {coletor}")


    async def _tratar_posse(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_estado_posse(topico)
        if pulseira is None or not self._acompanha(pulseira, topico):
            return
        dono = mensagem.conteudo["coletor"]
        if mensagem.conteudo["pulseira"] != pulseira or (dono is not None and (not isinstance(dono, str) or not dono)):
            logging.warning(f"Posse descartada | Tópico: {topico} | Conteúdo inválido: {mensagem.conteudo}")
            return
        self.visao.atualizar_posse(pulseira, dono)

        # Posse retida em um coletor que já caiu (ex: coordenador reiniciou)
        if dono is not None and dono in self.visao.coletores_offline:
            await self._liberar_posse_orfa(pulseira, dono)


    async def _liberar_posse_orfa(self, pulseira: str, coletor: str) -> None:
        """Coletor responsável ficou indisponível, então a posse é liberada (RF14)."""
        if (liberacao := self._liberacoes.get(pulseira)) is not None:
            liberacao.responder(True) # A queda vale como liberação para o handoff
            return
        if (espera := self._esperas_conexao.get(pulseira)) is not None and espera.coletor == coletor:
            espera.responder(False) # Caiu antes de conectar: quem decide é a disputa (RF14, RF06)
            return

        logging.info(f"Posse liberada | Pulseira: {pulseira} | Motivo: coletor {coletor} indisponível.")
        self._leituras_fracas.pop(pulseira, None)
        try:
            await self._publicar_posse(pulseira, None)
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {pulseira} | Posse não liberada: {e}")


    async def _tratar_sinal(self, topico: str, mensagem: Mensagem) -> None:
        ids = Topicos.extrair_estado_rssi(topico)
        if ids is None:
            return
        coletor, pulseira = ids
        if not self._acompanha(pulseira, topico):
            return

        try:
            if mensagem.origem != coletor:
                raise ValueError(f"origem {mensagem.origem!r} não é o coletor do tópico")
            if mensagem.conteudo["pulseira"] != pulseira:
                raise ValueError(f"pulseira {mensagem.conteudo['pulseira']!r} não é a do tópico")
            estado = EstadoConexao(mensagem.conteudo["estado"])
            rssi = _inteiro(mensagem.conteudo["rssi"], "rssi", nulo=True)
            # Só a varredura publica amostras (RF18): sem o campo, a leitura é da conexão ativa
            amostras = _inteiro(mensagem.conteudo.get("amostras"), "amostras", nulo=True)
            if amostras is not None and amostras < 1:
                raise ValueError(f"amostras {amostras} deve ser maior que zero")
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"Sinal descartado | {topico} | conteúdo inválido [{type(e).__name__}]: {e}")
            return
        if estado is EstadoConexao.CONEXAO_FRACA:
            # Quem classifica conexao-fraca é o coordenador (RF19); o coletor não publica esse estado (RF15)
            logging.warning(f"Sinal descartado | {topico} | estado {estado.value} não vem do coletor")
            return
        if not self._registrar_mensagem(coletor, topico, mensagem):
            return

        sinal = self.visao.registrar_sinal(pulseira, coletor, estado, rssi, amostras)
        if estado is EstadoConexao.CONECTADO:
            self._ultimo_ativo[pulseira] = coletor # Favorito
            if (espera := self._esperas_conexao.get(pulseira)) is not None and espera.coletor == coletor:
                espera.responder(True)
        valido = self.visao.sinal_valido(coletor, rssi, amostras)
        logging.debug(
            f"Sinal | Pulseira: {pulseira} | Coletor: {coletor} | {estado.value} | "
            f"RSSI: {rssi} | Amostras: {amostras} | Válido: {valido}"
        )
        await self._avaliar(pulseira, coletor, sinal, valido)


    async def _tratar_liberacao(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_liberacao_posse(topico)
        if pulseira is None or not self._acompanha(pulseira, topico):
            return
        handoff = mensagem.conteudo.get("handoff")
        if mensagem.conteudo["pulseira"] != pulseira or (handoff is not None and not isinstance(handoff, str)):
            logging.warning(f"Liberação descartada | Tópico: {topico} | Conteúdo inválido: {mensagem.conteudo}")
            return
        if not self._registrar_mensagem(mensagem.origem, topico, mensagem):
            return
        if mensagem.origem != self.visao.posse.get(pulseira):
            logging.warning(f"Liberação descartada | Pulseira: {pulseira} | {mensagem.origem} não é o dono.")
            return

        logging.info(
            f"Liberação recebida | Pulseira: {pulseira} | Coletor: {mensagem.origem}"
            + (f" | Handoff: {handoff}" if handoff else "")
        )
        # Qualquer liberação do dono conta, mesmo sem o id: a pulseira foi solta de qualquer jeito
        if (liberacao := self._liberacoes.get(pulseira)) is not None:
            # Resposta ao HANDOFF. O horário fica registrado aqui, e não em _conduzir_handoff, para
            # a disputa contar as leituras que cheguem antes de a tarefa retomar (RF06)
            liberacao.responder(True)
            return
        if (espera := self._esperas_conexao.get(pulseira)) is not None and espera.coletor == mensagem.origem:
            espera.responder(False) # Desistiu antes de conectar: quem decide é a disputa (RF14, RF06)
            return
        try:
            # Coletor desconectado de forma controlada (RF14)
            await self._publicar_posse(pulseira, None)
            logging.info(f"Posse liberada | Pulseira: {pulseira} | Motivo: liberação do coletor {mensagem.origem}")
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {pulseira} | Posse não liberada: {e}")


    async def _iniciar_handoff(self, pulseira: str, dono: str) -> None:
        """Soltar e disputar: Manda a origem soltar; o resto acontece em _conduzir_handoff.

        O id relaciona no log o comando, a liberação e o resultado (RF06).
        """
        id_handoff = uuid.uuid4().hex[:8] # Curto para o log; só precisa distinguir os handoffs entre si
        logging.info(
            f"Handoff iniciado | Id: {id_handoff} | Pulseira: {pulseira} | Origem: {dono} | Motivo: conexao-fraca "
            f"({self._leituras_fracas[pulseira]} leituras < {self.limiar} dBm)"
        )
        self._leituras_fracas[pulseira] = 0
        comando = Mensagem(
            tipo=TipoMensagem.HANDOFF,
            origem=self.broker.cliente_id,
            conteudo={
                "id": id_handoff,
                "pulseira": pulseira,
                "origem": dono,
            }
        )
        # Criado antes do HANDOFF: a liberação pode ser processada antes de a tarefa começar a rodar
        liberada = self._liberacoes[pulseira] = _Espera(dono)
        try:
            await self.broker.publicar(Topicos.handoff(pulseira), comando)
        except BrokerIndisponivel as e:
            self._liberacoes.pop(pulseira, None)
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Handoff não iniciado: {e}"
            )
            return
        self._disputas[pulseira] = asyncio.create_task(self._conduzir_handoff(pulseira, dono, liberada, id_handoff))


    async def _conduzir_handoff(self, pulseira: str, dono: str, liberada: _Espera, id_handoff: str) -> None:
        """Espera a origem liberar, publica a posse como liberada e abre a disputa"""
        try:
            try:
                await asyncio.wait_for(liberada.fim.wait(), self.prazo_liberacao)
            except TimeoutError:
                logging.warning(
                    f"Handoff cancelado | Id: {id_handoff} | Pulseira: {pulseira} | "
                    f"Origem {dono} não liberou em {self.prazo_liberacao:g} s; continua sendo dono."
                )
                return
            # Daqui em diante, uma liberação ou queda é a de quem recebeu a posse na disputa
            self._liberacoes.pop(pulseira, None)
            await self._publicar_posse(pulseira, None)
            logging.info(f"Posse liberada | Handoff: {id_handoff} | Pulseira: {pulseira} | Motivo: origem {dono} soltou a pulseira")
            logging.info(
                f"Disputa aberta | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Janela: {self.janela_disputa:g} s | Favorito: {self._ultimo_ativo.get(pulseira)}"
            )
            # Só contam as leituras que chegaram depois da liberação (RF06)
            await self._encerrar_disputa(pulseira, liberada.em, id_handoff, origem=dono)
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Handoff interrompido: {e}"
            )
        finally:
            self._liberacoes.pop(pulseira, None)
            self._disputas.pop(pulseira, None)


    async def anunciar_online(self) -> None:
        """Publica a presença online (retida) no encerramento controlado."""
        if self._encerrada:
            return
        if await self._publicar_presenca(EstadoPresenca.ONLINE):
            logging.info(f"Presença: {EstadoPresenca.ONLINE.value} | Coordenador ")


    async def anunciar_offline(self) -> None:
        """Publica a presença offline (retida) no encerramento controlado.

        Deve ser chamado antes de desconectar do broker: numa desconexão limpa o broker
        descarta a última vontade, então este é o único aviso de offline. Depois dele, 
        o sinal de vida para e as reconexões não anunciam mais online.
        """
        self._encerrada = True
        if await self._publicar_presenca(EstadoPresenca.OFFLINE):
            logging.info(f"Presença: {EstadoPresenca.OFFLINE.value} | Coordenador ")


    def mensagem(self, estado: EstadoPresenca) -> Mensagem:
        """Monta a mensagem de presença. O timestamp vão no envelope."""
        return Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": estado.value,
            },
        )


    async def manter_sinal_de_vida(self) -> None:
        """Publica o sinal de vida em tópico próprio, sem retenção, a cada `intervalo` segundos (RF19),
        até o encerramento ou cancelamento.

        Sem conexão com o broker o sinal é pulado; a falha já é registrada pela ConexaoBroker.
        """
        while not self._encerrada:
            await asyncio.sleep(self.intervalo)
            if self._encerrada or not self.broker.conectado:
                continue

            sinal = Mensagem(tipo=TipoMensagem.SINAL_DE_VIDA, origem=self.broker.cliente_id, conteudo={})
            try:
                await self.broker.publicar(Topicos.sinal_de_vida_coordenador(), sinal)
                logging.debug("Sinal de vida | Coordenador")
            except BrokerIndisponivel as e:
                logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Coordenador | Sinal de vida não publicado: {e}")


    async def manter_verificacao_coletores(self) -> None:
        """A cada INTERVALO_VERIFICACAO_COLETORES, considera indisponível o coletor sem mensagem há mais
        que o prazo e libera a posse retida em nome de quem não está disponível (RF13, RF14).

        Fica parada enquanto o coordenador está sem broker: sem receber nada, todos pareceriam sem sinal.
        """
        while not self._encerrada:
            await asyncio.sleep(INTERVALO_VERIFICACAO_COLETORES)
            if self._encerrada or not self._ouvindo:
                continue
            await self._verificar_coletores()


    async def _verificar_coletores(self, agora: float | None = None) -> None:
        agora = time.monotonic() if agora is None else agora
        for coletor in self.visao.expirar_sem_sinal(self.prazo_sinal_de_vida, agora):
            logging.warning(
                f"Coletor indisponível | Coletor: {coletor} | Motivo: sem sinal de vida há mais de {self.prazo_sinal_de_vida:g} s"
            )
        for pulseira, dono in self.visao.posses_orfas(self.prazo_sinal_de_vida, agora):
            await self._liberar_posse_orfa(pulseira, dono)


    def _reiniciar_contagem(self) -> None:
        """A cada (re)conexão ao broker, depois de refazer as assinaturas: o tempo sem broker não conta contra os coletores."""
        self.visao.reiniciar_contagem()
        self._ouvindo = True


    def _parar_contagem(self) -> None:
        """A sessão com o broker caiu: sem receber nada, nenhum coletor pode ser dado como sem sinal."""
        self._ouvindo = False


    async def registrar_visao(self) -> None:
        """Loga em DEBUG, a cada 10 s, como o coordenador enxerga a rede."""
        while not self._encerrada:
            await asyncio.sleep(10)
            logging.debug("%s", self.visao)


    async def _publicar_presenca(self, estado: EstadoPresenca) -> bool:
        """Publica a presença. Devolve False se o broker estiver indisponível, sem lançar erro."""
        try:
            await self.broker.publicar(Topicos.presenca_coordenador(), self.mensagem(estado))
            return True
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Coordenador: {self.broker.cliente_id} | "
                f"Não foi possível publicar a presença {estado.value}: {e}"
            )
            return False


    def _ultima_vontade(self) -> aiomqtt.Will:
        """Presença offline publicada pelo broker se o coordenador cair sem avisar.

        O timestamp é o do registro, não o da queda: para saber quando o coletor caiu,
        o coordenador deve usar o horário de recebimento.
        """
        mensagem = Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": EstadoPresenca.OFFLINE.value,
            },
        )
        config = mensagem.config_entrega
        return aiomqtt.Will(
                topic=Topicos.presenca_coordenador(),
                payload=mensagem.para_json(),
                qos=config.qos,
                retain=config.retida,
        )

    
    async def _avaliar(self, pulseira: str, coletor: str, sinal: SinalColetor, valido: bool) -> None:
        """Decide, a cada sinal, se a pulseira precisa de um coletor (novo ou outro).

        Args:
            valido (bool): Se a leitura é um sinal válido (RF14), que abre a disputa sem posse vigente.
        """
        if pulseira in self._disputas:
            return # Disputa, handoff ou espera da conexão do vencedor em andamento

        dono = self.visao.posse.get(pulseira)
        if dono is None:
            if valido:
                # A janela começa com o sinal que abriu a disputa, que também conta (RF14)
                self._abrir_disputa(pulseira, sinal.recebido_em)
            return

        if coletor != dono or not self._conexao_fraca(pulseira, sinal):
            return # Só a leitura do dono dispara handoff

        # Sem comparação prévia: a pulseira conectada não anuncia, então nenhum vizinho tem sinal.
        await self._iniciar_handoff(pulseira, dono)


    def _conexao_fraca(self, pulseira: str, sinal: SinalColetor) -> bool: 
        """Conexao-fraca após N leituras consecutivas do dono abaixo do limiar."""
        if sinal.estado is EstadoConexao.CONECTADO and sinal.rssi is not None and sinal.rssi < self.limiar:
            fracas = self._leituras_fracas.get(pulseira, 0) + 1
        else:
            fracas = 0
        self._leituras_fracas[pulseira] = fracas
        if fracas == self.minimo_fracas:
            logging.info(f"Estado: conexao-fraca | Pulseira: {pulseira} | RSSI: {sinal.rssi} dBm.")
        return fracas >= self.minimo_fracas


    def _abrir_disputa(self, pulseira: str, inicio: float) -> None:
        logging.info(
            f"Disputa aberta | Pulseira: {pulseira} | Janela: {self.janela_disputa:g} s | "
            f"Favorito: {self._ultimo_ativo.get(pulseira)}"
        )
        self._disputas[pulseira] = asyncio.create_task(self._encerrar_disputa(pulseira, inicio))


    async def _encerrar_disputa(
        self, pulseira: str, inicio: float, handoff: str | None = None, origem: str | None = None,
    ) -> None:
        """Ao fim da janela, concede a posse ao vencedor e espera que ele conecte (RF14).

        Args:
            inicio (float): time.monotonic() do início da janela; só contam as medianas a partir dele.
            handoff (str | None): Id do handoff que abriu a disputa, para o log do resultado (RF06).
            origem (str | None): Origem do handoff, que recebe a posse de volta se o vencedor não conectar (RF06).
        """
        ref = f" | Handoff: {handoff}" if handoff else ""
        try:
            await asyncio.sleep(self.janela_disputa)
            if self._posse_chegou_na_janela(pulseira, ref):
                return

            if not self.visao.candidatos(pulseira, inicio):
                if not await self._estender_janela(pulseira, inicio):
                    logging.info(
                        f"Disputa sem vencedor{ref} | Pulseira: {pulseira} | Nenhum sinal válido em "
                        f"{self.janela_disputa * self.fator_extensao:g} s; posse continua liberada."
                    )
                    return
                await asyncio.sleep(self.janela_disputa) # Janela completa a partir do primeiro sinal válido
                if self._posse_chegou_na_janela(pulseira, ref):
                    return

            favorito = self._ultimo_ativo.get(pulseira)
            candidatos = self.visao.candidatos(pulseira, inicio)
            vencedor = escolher_coletor(candidatos, favorito=favorito, margem=self.margem)
            if vencedor is None:
                logging.info(f"Disputa sem vencedor{ref} | Pulseira: {pulseira} | Os candidatos ficaram indisponíveis durante a janela.")
                return

            espera = await self._conceder(pulseira, vencedor)
            motivo = "favorito" if vencedor == favorito else "maior RSSI"
            rssis = ", ".join(
                f"{coletor}: {candidato.rssi_suavizado:.1f} dBm ({candidato.amostras} amostras)"
                for coletor, candidato in candidatos.items()
            )
            logging.info(
                f"Posse concedida{ref} | Pulseira: {pulseira} | Coletor: {vencedor} | Motivo: {motivo} | "
                f"Favorito: {favorito} | RSSI suavizado [{rssis}]"
            )
            if not await self._aguardar_conexao(pulseira, espera):
                await self._resolver_falha_conexao(pulseira, vencedor, origem, ref)
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo}{ref} | Pulseira: {pulseira} | Posse não publicada: {e}")
        finally:
            self._disputas.pop(pulseira, None)


    def _posse_chegou_na_janela(self, pulseira: str, ref: str) -> bool:
        """A posse retida pode chegar durante a janela quando o coordenador acabou de ligar (RF19)."""
        dono = self.visao.posse.get(pulseira)
        if dono is None:
            return False
        logging.info(f"Disputa encerrada{ref} | Pulseira: {pulseira} | A posse retida em nome de {dono} chegou durante a janela.")
        return True


    async def _conceder(self, pulseira: str, coletor: str) -> _Espera:
        """Publica a posse em nome do coletor e começa a esperar o estado conectado (RF14).

        A espera é registrada antes da publicação, para não perder um conectado que chegue logo.

        Raises:
            BrokerIndisponivel: Se a posse não puder ser publicada.
        """
        espera = self._esperas_conexao[pulseira] = _Espera(coletor)
        try:
            await self._publicar_posse(pulseira, coletor)
        except BrokerIndisponivel:
            self._esperas_conexao.pop(pulseira, None)
            raise
        return espera


    async def _aguardar_conexao(self, pulseira: str, espera: _Espera) -> bool:
        """Espera o coletor que recebeu a posse publicar o estado conectado dentro do prazo (RF14).

        Returns:
            bool: True se conectou; False se o prazo estourou ou se ele liberou a posse ou ficou
                indisponível antes de conectar.
        """
        try:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(espera.fim.wait(), self.prazo_conexao)
        finally:
            if self._esperas_conexao.get(pulseira) is espera:
                del self._esperas_conexao[pulseira]
        return espera.sucesso


    async def _resolver_falha_conexao(self, pulseira: str, coletor: str, origem: str | None, ref: str) -> None:
        """O coletor com a posse não conectou no prazo. No handoff, a posse volta à origem, que tem o
        mesmo prazo (RF06); fora dele, ou se a origem também falhar, a posse é liberada (RF14).

        Raises:
            BrokerIndisponivel: Se a posse não puder ser publicada.
        """
        if self.visao.posse.get(pulseira) != coletor:
            return # A posse já mudou por outro caminho

        if origem is not None and origem != coletor and self.visao.disponivel(origem):
            logging.warning(
                f"Posse devolvida à origem{ref} | Pulseira: {pulseira} | Origem: {origem} | "
                f"Motivo: {coletor} não conectou em {self.prazo_conexao:g} s"
            )
            espera = await self._conceder(pulseira, origem)
            if await self._aguardar_conexao(pulseira, espera):
                return
            if self.visao.posse.get(pulseira) != origem:
                return
            coletor = origem

        await self._publicar_posse(pulseira, None)
        # A próxima disputa abre com o próximo sinal válido, com o mesmo favorito
        logging.warning(
            f"Posse liberada{ref} | Pulseira: {pulseira} | Motivo: {coletor} não conectou em {self.prazo_conexao:g} s"
        )


    async def _publicar_posse(self, pulseira: str, coletor: str | None) -> None:
        """Publica a posse (retida) e já atualiza a visão, sem esperar o broker."""
        mensagem = Mensagem(
            tipo=TipoMensagem.POSSE,
            origem=self.broker.cliente_id,
            conteudo={
                "pulseira": pulseira,
                "coletor": coletor
            }
        )
        await self.broker.publicar(Topicos.estado_posse(pulseira), mensagem)
        self.visao.atualizar_posse(pulseira, coletor)


    async def _estender_janela(self, pulseira: str, inicio: float) -> bool:
        """Sem sinal válido ao fim da janela, espera o primeiro até a janela somar `fator_extensao`
        vezes o tamanho padrão (RF14).

        Returns:
            bool: True se algum coletor publicou um sinal válido dentro do limite.
        """
        prazo = inicio + self.janela_disputa * self.fator_extensao
        while not self.visao.candidatos(pulseira, inicio):
            if time.monotonic() >= prazo:
                return False
            await asyncio.sleep(INTERVALO_VERFICACAO)
        return True


    async def executar(self) -> None:
        """Inicia o coordenador."""
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.manter_sinal_de_vida()),
            asyncio.create_task(self.manter_verificacao_coletores()),
            asyncio.create_task(self.registrar_visao()),
        }
        try:
            concluidas, _ = await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
            for tarefa in concluidas:
                tarefa.result()
        finally:
            await self.anunciar_offline()
            tarefas |= set(self._disputas.values())  # Janelas de disputa/handoff em andamento
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)

        
