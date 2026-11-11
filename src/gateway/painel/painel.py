import asyncio
import logging
import os
import socket
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI
from bluetooth.scanner import ler_endereco_pulseira
from coordenador.coordenador import ler_limiar_conexao_fraca
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from painel.domain.sinal import LIMITE_BOM, LIMITE_OTIMO
from painel.domain.visao import JANELA_HISTORICO, TEMPO_SEM_LEITURA, VisaoPainel


JANELA_MEDIANA_PADRAO = 3   # Leituras de RSSI na mediana: 3, como na volta ao normal da conexao-fraca (RF19)
INTERVALO_ATUALIZACAO = 1.0 # s entre as atualizações enviadas aos navegadores (RF20)

VALORES_LIGADO    = {"1", "true", "sim", "s", "yes", "on"}
VALORES_DESLIGADO = {"", "0", "false", "nao", "não", "n", "no", "off"}

Servico = Callable[[], Awaitable[None]]


def identificador_painel() -> str:
    """Client id MQTT do painel: PAINEL_ID ou painel-<hostname>.

    O hostname evita que dois painéis na mesma rede usem o mesmo client id, o que faria o broker
    derrubar uma sessão sempre que a outra conecta.
    """
    return os.getenv("PAINEL_ID") or f"painel-{socket.gethostname()}"


def ler_pulseiras_painel(pulseiras: list[str] | None = None) -> list[str]:
    """Resolve as pulseiras exibidas pelo painel (RF20).

    Args:
        pulseiras (list[str] | None): Endereços MAC. Se omitido, são lidos de PULSEIRAS (separados por
            vírgula) ou, sem ela, de TARGET_ADDRESS, a pulseira do coletor (na versão atual há uma só).

    Returns:
        list[str]: Endereços em maiúsculas, sem repetição, na ordem configurada. Vazia se nada estiver configurado.

    Raises:
        ValueError: Se algum endereço não for um MAC válido.
    """
    if pulseiras is None:
        valor = os.getenv("PULSEIRAS") or os.getenv("TARGET_ADDRESS") or ""
        pulseiras = [pulseira for pulseira in valor.split(",") if pulseira.strip()]

    enderecos: list[str] = []
    for pulseira in pulseiras:
        try:
            endereco = ler_endereco_pulseira(pulseira)
        except ValueError as e:
            raise ValueError(
                f"Endereço de pulseira inválido: {pulseira!r} (PULSEIRAS ou TARGET_ADDRESS). "
                "Use endereços MAC no formato AA:BB:CC:DD:EE:FF, separados por vírgula."
            ) from e
        if endereco not in enderecos:
            enderecos.append(endereco)
    return enderecos


def ler_janela_mediana(janela: int | None = None) -> int:
    """Resolve quantas leituras de RSSI entram na mediana exibida em destaque (RF20).

    Args:
        janela (int | None): Número de leituras. Se omitido, é lido de PAINEL_JANELA_MEDIANA (padrão de 3).

    Raises:
        ValueError: Se não for um inteiro maior que zero.
    """
    if janela is None:
        valor = os.getenv("PAINEL_JANELA_MEDIANA") or str(JANELA_MEDIANA_PADRAO)
        try:
            janela = int(valor)
        except ValueError as e:
            raise ValueError(f"PAINEL_JANELA_MEDIANA deve ser um inteiro, recebido {valor!r}.") from e

    if janela <= 0:
        raise ValueError("PAINEL_JANELA_MEDIANA deve ser maior que zero.")
    return janela


def ler_modo_simulado(simulado: bool | None = None) -> bool:
    """Resolve se o painel sobe com o simulador (RF20).

    Args:
        simulado (bool | None): Se omitido, é lido de PAINEL_SIMULADO (1/true/sim liga; 0/false/nao ou vazio desliga).

    Raises:
        ValueError: Se o valor de PAINEL_SIMULADO não for reconhecido.
    """
    if simulado is not None:
        return simulado
    valor = os.getenv("PAINEL_SIMULADO", "").strip().lower()
    if valor in VALORES_LIGADO:
        return True
    if valor in VALORES_DESLIGADO:
        return False
    raise ValueError(f"PAINEL_SIMULADO deve ser 1 ou 0 (true/false), recebido {valor!r}.")


class Painel:
    """Consumidor do broker que monta a visão das pulseiras e alimenta os navegadores (RF20).

    Não se conecta às pulseiras nem depende da máquina de um coletor: frequência cardíaca (RF17),
    estado/RSSI (RF09) e posse (RF14) chegam só pelo broker.
    """

    def __init__(
        self,
        broker: ConexaoBroker,
        pulseiras: list[str],
        limiar: int | None = None,
        janela_mediana: int | None = None,
        simulado: bool = False,
    ) -> None:
        """
        Args:
            broker (ConexaoBroker): Sessão MQTT do painel.
            pulseiras (list[str]): Endereços das pulseiras exibidas (ver `ler_pulseiras_painel`).
            limiar (int | None): Divisa entre fraco e crítico, em dBm. Se omitido, é o limiar de
                conexao-fraca do coordenador, lido de LIMIAR_CONEXAO_FRACA (padrão de -78 dBm).
            janela_mediana (int | None): Leituras de RSSI na mediana em destaque. Se omitido, é lido
                de PAINEL_JANELA_MEDIANA (padrão de 3).
            simulado (bool): Só para a página indicar que os dados são simulados.

        Raises:
            ValueError: Sem pulseiras, ou com limiar ou janela da mediana inválidos.
        """
        if not pulseiras:
            raise ValueError("O painel precisa de ao menos uma pulseira. Defina PULSEIRAS ou TARGET_ADDRESS.")
        self.broker = broker
        self.simulado = simulado
        self.visao = VisaoPainel(pulseiras, ler_limiar_conexao_fraca(limiar), ler_janela_mediana(janela_mediana))
        self.broker.ao_receber = self.ao_receber
        self._atualizacao = asyncio.Condition()


    async def assinar_topicos(self) -> None:
        """Assina, para cada pulseira configurada, a posse, a frequência cardíaca e o estado/RSSI de
        todos os coletores.

        Pode ser chamado antes de conectar: a ConexaoBroker refaz as assinaturas a cada (re)conexão, e
        o broker entrega na hora a posse retida, então o painel conhece o dono logo ao abrir e depois de
        um handoff que aconteceu com ele fora do ar.
        """
        for pulseira in self.visao.pulseiras:
            await self.broker.assinar(Topicos.estado_posse(pulseira))
            await self.broker.assinar(Topicos.estado_rssi("+", pulseira), qos=0)
            await self.broker.assinar(Topicos.frequencia_cardiaca(pulseira), qos=0)


    def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        """Encaminha cada mensagem pelo tipo; o tópico identifica a pulseira e o coletor."""
        agora = time.monotonic()
        match mensagem.tipo:
            case TipoMensagem.POSSE:
                self._tratar_posse(topico, mensagem)
            case TipoMensagem.ESTADO_PULSEIRA:
                self._tratar_estado_rssi(topico, mensagem, agora)
            case TipoMensagem.FREQUENCIA_CARDIACA:
                self._tratar_frequencia_cardiaca(topico, mensagem, agora)


    def configuracao(self) -> dict:
        """Parâmetros que a página usa para desenhar faixas, prazos e janelas."""
        return {
            "pulseiras": self.visao.pulseiras,
            "limiar": self.visao.limiar,
            "limite_otimo": LIMITE_OTIMO,
            "limite_bom": LIMITE_BOM,
            "tempo_sem_leitura": TEMPO_SEM_LEITURA,
            "janela_historico": JANELA_HISTORICO,
            "janela_mediana": self.visao.janela_mediana,
            "simulado": self.simulado,
        }


    def evento_inicial(self) -> tuple[dict, int]:
        """Tudo o que um navegador precisa ao abrir ou reconectar: configuração, retrato e histórico.

        Returns:
            tuple[dict, int]: O evento e o seq do registro mais recente incluído nele.
        """
        historico = {
            pulseira: [registro.como_dict() for registro in registros]
            for pulseira, registros in self.visao.historico().items()
        }
        evento = {"config": self.configuracao(), "retrato": self._retrato(), "historico": historico}
        return evento, self.visao.ultimo_seq


    def evento_atualizacao(self, desde_seq: int) -> tuple[dict, int]:
        """Retrato atual e os registros que chegaram depois de `desde_seq`.

        Cada navegador guarda o próprio `desde_seq`: um navegador lento recebe de uma vez tudo o que
        perdeu, em vez de perder registros.

        Returns:
            tuple[dict, int]: O evento e o seq do registro mais recente incluído nele.
        """
        novos = {
            pulseira: [registro.como_dict() for registro in registros]
            for pulseira, registros in self.visao.registros_desde(desde_seq).items()
        }
        return {"retrato": self._retrato(), "novos": novos}, self.visao.ultimo_seq


    def zerar_estatisticas(self, pulseira: str) -> bool:
        """Recomeça mínimo, média e máximo da frequência cardíaca da pulseira (pedido do operador)."""
        if not self.visao.zerar_estatisticas(pulseira):
            return False
        logging.info(f"Painel | Pulseira: {pulseira} | Estatísticas de frequência cardíaca zeradas")
        return True


    async def aguardar_atualizacao(self) -> None:
        """Espera a próxima atualização periódica (uma por segundo)."""
        async with self._atualizacao:
            await self._atualizacao.wait()


    async def manter_atualizacao(self) -> None:
        """A cada segundo, descarta o histórico com mais de 15 min e acorda os navegadores (RF20)."""
        while True:
            await asyncio.sleep(INTERVALO_ATUALIZACAO)
            self.visao.podar(time.monotonic())
            async with self._atualizacao:
                self._atualizacao.notify_all()


    async def executar(self, *servicos: Servico) -> None:
        """Mantém a sessão com o broker e as atualizações, junto dos serviços recebidos, até ser cancelado.

        Args:
            servicos (Servico): Corrotinas de longa duração que rodam com o painel (servidor web e,
                no modo simulado, o simulador). Se uma delas falhar (ex.: porta ocupada), o painel
                encerra com o erro.
        """
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.manter_atualizacao()),
            *(asyncio.create_task(servico()) for servico in servicos),
        }
        try:
            concluidas, _ = await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
            for tarefa in concluidas:
                tarefa.result()
        finally:
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)


    def _retrato(self) -> dict:
        return {
            "t": datetime.now(timezone.utc).timestamp(),
            # A queda do broker não fecha o painel: a página avisa e a ConexaoBroker reconecta sozinha (RF20)
            "broker": self.broker.conectado,
            "pulseiras": {r.pulseira: r.como_dict() for r in self.visao.retrato(time.monotonic())},
        }


    def _tratar_posse(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_estado_posse(topico)
        if pulseira is None or not self.visao.acompanha(pulseira):
            return
        coletor = mensagem.conteudo["coletor"]
        if coletor is not None and (not isinstance(coletor, str) or not coletor):
            logging.warning(f"Posse descartada | Tópico: {topico} | coletor {coletor!r} inválido")
            return
        if self.visao.registrar_posse(pulseira, coletor):
            logging.info(f"Painel | Pulseira: {pulseira} | Posse: {coletor or 'liberada'}")


    def _tratar_estado_rssi(self, topico: str, mensagem: Mensagem, agora: float) -> None:
        ids = Topicos.extrair_estado_rssi(topico)
        if ids is None:
            return
        coletor, pulseira = ids
        if not self.visao.acompanha(pulseira):
            return
        try:
            if mensagem.origem != coletor:
                raise ValueError(f"origem {mensagem.origem!r} não é o coletor do tópico")
            if mensagem.conteudo["pulseira"] != pulseira:
                raise ValueError(f"pulseira {mensagem.conteudo['pulseira']!r} não é a do tópico")
            leitura = LeituraRSSI(
                endereco=pulseira,
                estado=EstadoConexao(mensagem.conteudo["estado"]),
                rssi=_inteiro_ou_nulo(mensagem.conteudo["rssi"], "rssi"),
                timestamp=mensagem.timestamp,
            )
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"Estado descartado | Tópico: {topico} | Conteúdo inválido [{type(e).__name__}]: {e}")
            return
        # Devolve None para o RSSI da varredura dos coletores sem posse, que o painel não exibe
        self.visao.registrar_estado_rssi(coletor, leitura, agora)


    def _tratar_frequencia_cardiaca(self, topico: str, mensagem: Mensagem, agora: float) -> None:
        pulseira = Topicos.extrair_frequencia_cardiaca(topico)
        if pulseira is None or not self.visao.acompanha(pulseira):
            return
        try:
            coletor = mensagem.conteudo["coletor"]
            if coletor != mensagem.origem:
                raise ValueError(f"coletor {coletor!r} não é a origem {mensagem.origem!r}")
            if mensagem.conteudo["pulseira"] != pulseira:
                raise ValueError(f"pulseira {mensagem.conteudo['pulseira']!r} não é a do tópico")
            bpm = _inteiro_ou_nulo(mensagem.conteudo["bpm"], "bpm")
            if bpm is None or bpm <= 0:
                raise ValueError(f"bpm {bpm!r} deve ser maior que zero")
            # Os intervalos RR não são exibidos no painel (RF20)
            leitura = LeituraFrequenciaCardiaca(bpm=bpm, timestamp=mensagem.timestamp)
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(
                f"Frequência cardíaca descartada | Tópico: {topico} | Conteúdo inválido [{type(e).__name__}]: {e}"
            )
            return
        if self.visao.registrar_frequencia_cardiaca(pulseira, coletor, leitura, agora) is None:
            logging.debug(
                f"Frequência cardíaca repetida ou fora de ordem descartada | Pulseira: {pulseira} | "
                f"Coletor: {coletor} | Timestamp: {mensagem.timestamp.isoformat()}"
            )


def _inteiro_ou_nulo(valor: object, campo: str) -> int | None:
    # type() e não isinstance(): bool é subclasse de int (mesmo cuidado do Mensagem.de_json)
    if valor is not None and type(valor) is not int:
        raise TypeError(f"{campo} {valor!r} não é inteiro")
    return valor
