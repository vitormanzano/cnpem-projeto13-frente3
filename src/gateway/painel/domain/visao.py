from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from statistics import median

from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI
from painel.domain.sinal import QualidadeSinal, classificar_sinal


TEMPO_SEM_LEITURA = 5.0       # s sem atualização até o valor deixar de ser exibido como atual (RF20)
JANELA_HISTORICO  = 15 * 60.0 # s guardados: a maior janela dos gráficos (RF20)
MAXIMO_REGISTROS  = 10_000    # Teto por pulseira, caso as leituras cheguem bem mais rápido que uma por segundo


@dataclass(frozen=True)
class Registro:
    """Uma mensagem recebida do broker e guardada no histórico do painel (RF20).

    Tem `bpm` (frequência cardíaca, RF17) ou `estado` (estado e RSSI do coletor com a posse, RF09).
    """
    seq: int              # Ordem de chegada no painel: o navegador recebe só os registros novos
    recebido_em: float    # time.monotonic() do recebimento, usado na poda do histórico
    coletor: str
    timestamp: datetime   # Horário da leitura no coletor (RF16), usado no gráfico e na tabela
    bpm: int | None = None
    estado: EstadoConexao | None = None
    rssi: int | None = None
    mediana: float | None = None

    def como_dict(self) -> dict:
        """Formato enviado ao navegador, com `t` em segundos desde a época (UTC)."""
        dados: dict = {"seq": self.seq, "t": self.timestamp.timestamp(), "coletor": self.coletor}
        if self.bpm is not None:
            dados["bpm"] = self.bpm
        if self.estado is not None:
            dados.update(estado=self.estado.value, rssi=self.rssi, mediana=self.mediana)
        return dados


@dataclass
class EstatisticasFC:
    """Frequência cardíaca mínima, média e máxima das leituras recebidas desde `desde` (RF20).

    `desde` é a abertura do painel ou o último "Zerar" do operador: nenhuma mensagem informa quando
    o coletor começou, e o painel só conhece as leituras que recebeu.
    """
    desde: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    minimo: int | None = None
    maximo: int | None = None
    soma: int = 0
    quantidade: int = 0

    @property
    def media(self) -> float | None:
        return self.soma / self.quantidade if self.quantidade else None

    def registrar(self, bpm: int) -> None:
        self.minimo = bpm if self.minimo is None else min(self.minimo, bpm)
        self.maximo = bpm if self.maximo is None else max(self.maximo, bpm)
        self.soma += bpm
        self.quantidade += 1

    def como_dict(self) -> dict:
        media = self.media
        return {
            "minimo": self.minimo,
            "media": None if media is None else round(media, 1),
            "maximo": self.maximo,
            "quantidade": self.quantidade,
            "desde": self.desde.timestamp(),
        }


@dataclass(frozen=True)
class RetratoPulseira:
    """O que o painel exibe de uma pulseira num instante (RF20). Valores sem atualização há mais de
    TEMPO_SEM_LEITURA vêm como None, e o navegador mostra "sem leitura"."""
    pulseira: str
    posse_conhecida: bool          # Já chegou alguma mensagem de posse (a retida chega ao assinar)
    dono: str | None               # Coletor com a posse (RF14); None com a posse liberada
    estado: EstadoConexao | None   # Último estado publicado pelo dono (RF08)
    estado_idade: float | None     # s desde esse estado
    bpm: int | None
    bpm_idade: float | None        # s desde a última FC recebida, mesmo que já não seja atual
    rssi: int | None               # Bruto, a última leitura do dono
    rssi_mediana: float | None     # Exibido em destaque
    qualidade: QualidadeSinal | None
    estatisticas: EstatisticasFC

    def como_dict(self) -> dict:
        return {
            "pulseira": self.pulseira,
            "posse_conhecida": self.posse_conhecida,
            "dono": self.dono,
            "estado": None if self.estado is None else self.estado.value,
            "estado_idade": _arredondar(self.estado_idade),
            "bpm": self.bpm,
            "bpm_idade": _arredondar(self.bpm_idade),
            "rssi": self.rssi,
            "rssi_mediana": self.rssi_mediana,
            "qualidade": None if self.qualidade is None else self.qualidade.value,
            "estatisticas": self.estatisticas.como_dict(),
        }


@dataclass
class AcompanhamentoPulseira:
    """O que o painel sabe de uma pulseira, montado só a partir das mensagens do broker."""
    endereco: str
    janela_mediana: int
    posse_conhecida: bool = False
    dono: str | None = None
    estado: EstadoConexao | None = None
    estado_recebido_em: float | None = None
    rssi: int | None = None
    rssi_recebido_em: float | None = None
    rssis: deque[int] = field(init=False)
    bpm: int | None = None
    bpm_recebido_em: float | None = None
    ultimo_timestamp_fc: datetime | None = None
    estatisticas: EstatisticasFC = field(default_factory=EstatisticasFC)
    registros: deque[Registro] = field(default_factory=lambda: deque(maxlen=MAXIMO_REGISTROS))

    def __post_init__(self) -> None:
        self.rssis = deque(maxlen=self.janela_mediana)

    def esquecer_sinal(self) -> None:
        """Estado e RSSI são do coletor, não da pulseira: sem o dono que os publicou, não valem mais."""
        self.estado = self.estado_recebido_em = None
        self.esquecer_rssi()

    def esquecer_rssi(self) -> None:
        self.rssi = self.rssi_recebido_em = None
        self.rssis.clear()


class VisaoPainel:
    """Estado das pulseiras configuradas, montado a partir das mensagens do broker (RF20).

    Só guarda e calcula: não acessa o broker nem o relógio. Quem chama passa `agora`
    (time.monotonic()), o que permite testar os prazos sem esperar.
    """

    def __init__(self, pulseiras: list[str], limiar: int, janela_mediana: int) -> None:
        """
        Args:
            pulseiras (list[str]): Endereços das pulseiras exibidas; mensagens de outras são ignoradas.
            limiar (int): Limiar de conexao-fraca em dBm (RF19), divisa entre fraco e crítico.
            janela_mediana (int): Leituras de RSSI na mediana exibida em destaque.
        """
        self.limiar = limiar
        self.janela_mediana = janela_mediana
        self._pulseiras = {p: AcompanhamentoPulseira(p, janela_mediana) for p in pulseiras}
        self._seq = 0

    @property
    def pulseiras(self) -> list[str]:
        return list(self._pulseiras)

    @property
    def ultimo_seq(self) -> int:
        """Seq do registro mais recente; 0 se nada chegou."""
        return self._seq

    def acompanha(self, pulseira: str) -> bool:
        return pulseira in self._pulseiras

    def registrar_posse(self, pulseira: str, coletor: str | None) -> bool:
        """Atualiza o coletor com a posse (RF14).

        Returns:
            bool: True se a posse mudou (ou se é a primeira recebida).
        """
        acompanhamento = self._pulseiras.get(pulseira)
        if acompanhamento is None:
            return False
        mudou = not acompanhamento.posse_conhecida or coletor != acompanhamento.dono
        acompanhamento.posse_conhecida = True
        if coletor != acompanhamento.dono:
            # Na troca de dono (handoff ou queda), a mediana não pode misturar o sinal de dois coletores
            acompanhamento.dono = coletor
            acompanhamento.esquecer_sinal()
        return mudou

    def registrar_estado_rssi(self, coletor: str, leitura: LeituraRSSI, agora: float) -> Registro | None:
        """Guarda o estado e o RSSI publicados pelo coletor com a posse (RF08, RF09).

        Args:
            coletor (str): Coletor que publicou (o do tópico).
            leitura (LeituraRSSI): Estado, RSSI e timestamp da mensagem.
            agora (float): time.monotonic() do recebimento.

        Returns:
            Registro | None: None se a pulseira não for acompanhada ou se o coletor não detiver a posse:
                os coletores sem posse publicam no mesmo padrão de tópico o RSSI da varredura (RF18).
        """
        acompanhamento = self._pulseiras.get(leitura.endereco)
        if acompanhamento is None or acompanhamento.dono is None or coletor != acompanhamento.dono:
            return None

        acompanhamento.estado = leitura.estado
        acompanhamento.estado_recebido_em = agora
        if leitura.estado is EstadoConexao.CONECTADO and leitura.rssi is not None:
            acompanhamento.rssis.append(leitura.rssi)
            acompanhamento.rssi = leitura.rssi
            acompanhamento.rssi_recebido_em = agora
        else:
            # Sem conexão, as leituras anteriores não representam mais o sinal
            acompanhamento.esquecer_rssi()

        return self._guardar(acompanhamento, Registro(
            seq=self._proximo_seq(),
            recebido_em=agora,
            coletor=coletor,
            timestamp=leitura.timestamp,
            estado=leitura.estado,
            rssi=leitura.rssi,
            mediana=_mediana(acompanhamento.rssis),
        ))

    def registrar_frequencia_cardiaca(
        self,
        pulseira: str,
        coletor: str,
        leitura: LeituraFrequenciaCardiaca,
        agora: float,
    ) -> Registro | None:
        """Guarda uma leitura de frequência cardíaca (RF17) e atualiza as estatísticas.

        A FC não é filtrada pela posse: só o coletor ativo a publica (RF17), e no handoff as leituras
        guardadas pela origem saem antes da liberação (RF06), ainda com a posse em nome dela.

        Returns:
            Registro | None: None se a pulseira não for acompanhada, ou se a leitura for repetida ou
                fora de ordem. O timestamp é o do recebimento no coletor (RF16) e só cresce.
        """
        acompanhamento = self._pulseiras.get(pulseira)
        if acompanhamento is None:
            return None
        if acompanhamento.ultimo_timestamp_fc is not None and leitura.timestamp <= acompanhamento.ultimo_timestamp_fc:
            return None

        acompanhamento.ultimo_timestamp_fc = leitura.timestamp
        acompanhamento.bpm = leitura.bpm
        acompanhamento.bpm_recebido_em = agora
        acompanhamento.estatisticas.registrar(leitura.bpm)
        return self._guardar(acompanhamento, Registro(
            seq=self._proximo_seq(),
            recebido_em=agora,
            coletor=coletor,
            timestamp=leitura.timestamp,
            bpm=leitura.bpm,
        ))

    def zerar_estatisticas(self, pulseira: str) -> bool:
        """Recomeça a contagem de mínimo, média e máximo. Returns: False se a pulseira não for acompanhada."""
        acompanhamento = self._pulseiras.get(pulseira)
        if acompanhamento is None:
            return False
        acompanhamento.estatisticas = EstatisticasFC()
        return True

    def retrato(self, agora: float) -> list[RetratoPulseira]:
        """O que exibir de cada pulseira, na ordem configurada."""
        return [self._retratar(acompanhamento, agora) for acompanhamento in self._pulseiras.values()]

    def historico(self) -> dict[str, list[Registro]]:
        """Todos os registros guardados, por pulseira, do mais antigo ao mais novo."""
        return {pulseira: list(a.registros) for pulseira, a in self._pulseiras.items()}

    def registros_desde(self, seq: int) -> dict[str, list[Registro]]:
        """Registros com seq maior que `seq`, por pulseira, do mais antigo ao mais novo."""
        resultado: dict[str, list[Registro]] = {}
        for pulseira, acompanhamento in self._pulseiras.items():
            novos: list[Registro] = []
            for registro in reversed(acompanhamento.registros):
                if registro.seq <= seq:
                    break
                novos.append(registro)
            resultado[pulseira] = novos[::-1]
        return resultado

    def podar(self, agora: float) -> None:
        """Descarta os registros recebidos há mais de JANELA_HISTORICO."""
        limite = agora - JANELA_HISTORICO
        for acompanhamento in self._pulseiras.values():
            while acompanhamento.registros and acompanhamento.registros[0].recebido_em < limite:
                acompanhamento.registros.popleft()

    def _retratar(self, acompanhamento: AcompanhamentoPulseira, agora: float) -> RetratoPulseira:
        bpm_idade = _idade(acompanhamento.bpm_recebido_em, agora)
        rssi_idade = _idade(acompanhamento.rssi_recebido_em, agora)
        bpm_atual = bpm_idade is not None and bpm_idade <= TEMPO_SEM_LEITURA
        rssi_atual = rssi_idade is not None and rssi_idade <= TEMPO_SEM_LEITURA
        mediana = _mediana(acompanhamento.rssis) if rssi_atual else None
        return RetratoPulseira(
            pulseira=acompanhamento.endereco,
            posse_conhecida=acompanhamento.posse_conhecida,
            dono=acompanhamento.dono,
            # O estado é o último publicado, com a idade: o coletor só o republica quando muda ou a cada
            # RSSI lido, e "desconectado" durante a reconexão (RF05) precisa continuar visível
            estado=acompanhamento.estado,
            estado_idade=_idade(acompanhamento.estado_recebido_em, agora),
            bpm=acompanhamento.bpm if bpm_atual else None,
            bpm_idade=bpm_idade,
            rssi=acompanhamento.rssi if rssi_atual else None,
            rssi_mediana=mediana,
            # Classifica o valor em destaque, a mediana (RF20)
            qualidade=classificar_sinal(mediana, self.limiar),
            estatisticas=replace(acompanhamento.estatisticas),
        )

    def _proximo_seq(self) -> int:
        self._seq += 1
        return self._seq

    @staticmethod
    def _guardar(acompanhamento: AcompanhamentoPulseira, registro: Registro) -> Registro:
        acompanhamento.registros.append(registro)
        return registro


def _mediana(rssis: deque[int]) -> float | None:
    return float(median(rssis)) if rssis else None


def _idade(recebido_em: float | None, agora: float) -> float | None:
    return None if recebido_em is None else max(0.0, agora - recebido_em)


def _arredondar(valor: float | None) -> float | None:
    return None if valor is None else round(valor, 1)
