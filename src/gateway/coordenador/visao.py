from collections import deque
from dataclasses import dataclass
from datetime import datetime
import time
from statistics import fmean

from bluetooth.domain.models import EstadoConexao


MINIMO_AMOSTRAS_PADRAO = 5  # Talvez precise de ajuste (medir a quantidade de anúncios que chega)
# Medianas guardadas por coletor: precisam cobrir a disputa mais longa, a janela estendida
# (janela x fator) mais a janela completa a partir do primeiro sinal (3 s x 3 + 3 s com os padrões)
MEDIANAS_GUARDADAS = 60


@dataclass(frozen=True)
class Mediana:
    """Mediana de RSSI publicada pela varredura de um coletor (RF18)."""
    rssi: int
    amostras: int       # Anúncios que compõem a mediana
    recebida_em: float  # time.monotonic() da chegada ao coordenador


@dataclass(frozen=True)
class Candidato:
    """Resumo dos sinais válidos de um coletor numa janela de disputa (RF14)."""
    rssi_suavizado: float  # Média das medianas válidas, ponderada pelas amostras de cada uma
    amostras: int          # Soma das amostras das medianas válidas
    primeira_em: float     # Chegada da primeira mediana válida na janela, para o desempate


@dataclass
class SinalColetor:
    """O que o coordenador sabe do sinal de um coletor para uma pulseira"""
    estado: EstadoConexao
    recebido_em: float
    rssi: int | None          # Último RSSI recebido; None em nao-encontrado
    medianas: deque[Mediana]  # Só as da varredura: a conexão ativa não publica mediana


    def __str__(self) -> str:
        rssi = "-" if self.rssi is None else f"{self.rssi} dBm"
        idade = time.monotonic() - self.recebido_em
        ultimas = [(m.rssi, m.amostras) for m in list(self.medianas)[-5:]]
        return f"{self.estado.value:<14} | rssi: {rssi:>8} | há {idade:4.1f}s | medianas (rssi, amostras): {ultimas}"


class VisaoColetores:
    """Estado da rede montado a partir das mensagens do broker: quem está disponível,
    quem enxerga cada pulseira (e com que sinal) e quem tem a posse (RF13)."""

    def __init__(self, minimo_amostras: int = MINIMO_AMOSTRAS_PADRAO) -> None:
        """
        Args:
            minimo_amostras (int): Anúncios que uma mediana precisa ter para o sinal ser válido (RF14).
        """
        self.minimo_amostras = minimo_amostras
        self.coletores_online: set[str] = set()
        self.coletores_offline: set[str] = set() # Anunciaram offline
        self.coletores_sem_sinal: set[str] = set() # Online, mas sem mensagem há mais que o prazo
        self.pulseira_atendida: dict[str, str | None] = {} # coletor -> pulseira informada na presença
        self.ultima_mensagem: dict[str, float] = {} # coletor -> time.monotonic() da última mensagem, de qualquer tópico
        self.ultimo_timestamp: dict[str, datetime] = {} # coletor -> maior timestamp de envelope recebido
        self.sinais: dict[str, dict[str, SinalColetor]] = {} # pulseira -> coletor -> sinal
        self.posse: dict[str, str | None] = {} # pulseira -> coletor responsável
        self._inicio_contagem = time.monotonic() # Referência do prazo de quem ainda não mandou nada


    def atualizar_presenca(self, coletor: str, online: bool, pulseira: str | None = None) -> bool:
        """Atualiza a presença do coletor e a pulseira que ele diz atender.

        Returns:
            bool: True se o estado (online/offline) mudou.
        """
        if online:
            mudou = coletor not in self.coletores_online
            self.coletores_online.add(coletor)
            self.coletores_offline.discard(coletor)
            self.pulseira_atendida[coletor] = pulseira
            return mudou

        mudou = coletor in self.coletores_online
        self.coletores_online.discard(coletor)
        self.coletores_offline.add(coletor)
        self.coletores_sem_sinal.discard(coletor)
        self.pulseira_atendida[coletor] = None
        # A sessão acabou: a próxima pode vir com o relógio atrás (Raspberry sem RTC reiniciando)
        self.ultimo_timestamp.pop(coletor, None)
        # Leituras antigas de um coletor offline não podem vencer uma disputa.
        for sinais in self.sinais.values():
            sinais.pop(coletor, None)
        return mudou


    def registrar_mensagem(
        self,
        coletor: str,
        timestamp: datetime,
        agora: float | None = None,
        aceitar_antiga: bool = False,
    ) -> bool:
        """Registra que chegou uma mensagem do coletor, de qualquer tópico (RF13).

        Args:
            coletor (str): Coletor que publicou.
            timestamp (datetime): Do envelope, no relógio do coletor; só é comparado com os do mesmo coletor.
            agora (float | None): time.monotonic() do recebimento. Se omitido, usa o atual.
            aceitar_antiga (bool): Registra mesmo mais antiga que a última, sem recuar o último timestamp.

        Returns:
            bool: False se o timestamp for mais antigo que o último recebido do coletor; a mensagem
                deve ser ignorada e nada é registrado.
        """
        ultimo = self.ultimo_timestamp.get(coletor)
        if ultimo is not None and timestamp < ultimo:
            if not aceitar_antiga:
                return False
        else:
            self.ultimo_timestamp[coletor] = timestamp
        self.ultima_mensagem[coletor] = time.monotonic() if agora is None else agora
        self.coletores_sem_sinal.discard(coletor)
        return True


    def disponivel(self, coletor: str) -> bool:
        """Presença online e mensagem dentro do prazo de sinal de vida (RF13)."""
        return coletor in self.coletores_online and coletor not in self.coletores_sem_sinal


    def expirar_sem_sinal(self, prazo: float, agora: float | None = None) -> list[str]:
        """Marca como sem sinal os coletores online sem mensagem há mais que `prazo` (RF13).

        Returns:
            list[str]: Os que ficaram indisponíveis nesta chamada; os já marcados não se repetem.
        """
        agora = time.monotonic() if agora is None else agora
        expirados = sorted(
            coletor for coletor in self.coletores_online - self.coletores_sem_sinal
            if agora - self.ultima_mensagem.get(coletor, self._inicio_contagem) > prazo
        )
        self.coletores_sem_sinal.update(expirados)
        return expirados


    def posses_orfas(self, prazo: float, agora: float | None = None) -> list[tuple[str, str]]:
        """Posses em nome de um coletor indisponível: a posse só vale com o responsável online (RF13).

        O responsável sem presença conhecida ganha `prazo` desde o início da contagem, porque a
        posse retida pode chegar antes da presença retida.

        Returns:
            list[tuple[str, str]]: (pulseira, coletor responsável).
        """
        agora = time.monotonic() if agora is None else agora
        em_tolerancia = agora - self._inicio_contagem <= prazo
        orfas = []
        for pulseira, dono in self.posse.items():
            if dono is None or self.disponivel(dono):
                continue
            presenca_conhecida = dono in self.coletores_online or dono in self.coletores_offline
            if em_tolerancia and not presenca_conhecida:
                continue
            orfas.append((pulseira, dono))
        return orfas


    def reiniciar_contagem(self, agora: float | None = None) -> None:
        """Recomeça os prazos de sinal de vida. Chamado a cada (re)conexão do coordenador ao broker:
        o tempo em que ele não recebia nada não pode contar contra os coletores."""
        agora = time.monotonic() if agora is None else agora
        self._inicio_contagem = agora
        for coletor in self.ultima_mensagem:
            self.ultima_mensagem[coletor] = agora


    def registrar_sinal(
        self, pulseira: str, coletor: str, estado: EstadoConexao, rssi: int | None, amostras: int | None = None,
    ) -> SinalColetor:
        """Registra uma leitura de estado/RSSI do coletor.

        Args:
            amostras (int | None): Anúncios da mediana, só na varredura (RF18). None na conexão ativa,
                cuja leitura atualiza o último RSSI mas não é mediana.
        """
        agora = time.monotonic()
        sinais = self.sinais.setdefault(pulseira, {})
        sinal = sinais.get(coletor)
        if sinal is None:
            sinal = SinalColetor(estado, agora, None, deque(maxlen=MEDIANAS_GUARDADAS))
            sinais[coletor] = sinal

        sinal.estado, sinal.recebido_em, sinal.rssi = estado, agora, rssi
        # Um nao-encontrado não apaga as medianas: as publicadas na janela continuam valendo (RF14)
        if rssi is not None and amostras is not None:
            sinal.medianas.append(Mediana(rssi, amostras, agora))
        return sinal


    def sinal_valido(self, coletor: str, rssi: int | None, amostras: int | None) -> bool:
        """Uma leitura é sinal válido se vem de coletor disponível e é uma mediana com o mínimo de amostras (RF14)."""
        return (
            rssi is not None and amostras is not None and amostras >= self.minimo_amostras
            and self.disponivel(coletor)
        )


    def atualizar_posse(self, pulseira: str, coletor: str | None) -> None:
        self.posse[pulseira] = coletor


    def pulseiras_de(self, coletor: str) -> list[str]:
        """Pulseiras cuja posse está com o coletor."""
        return [pulseira for pulseira, dono in self.posse.items() if dono == coletor]


    def candidatos(self, pulseira: str, desde: float) -> dict[str, Candidato]:
        """Coletores com sinal válido na janela de disputa iniciada em `desde` (RF14).

        Só contam as medianas com o mínimo de amostras que chegaram a partir de `desde`, de
        coletores disponíveis no momento da consulta. O filtro de disponibilidade é feito aqui, e
        não no registro, porque o MQTT só garante ordem dentro de um mesmo tópico: o RSSI de um
        coletor pode chegar antes da presença dele.

        Args:
            desde (float): time.monotonic() do início da janela (inclusive).

        Returns:
            dict[str, Candidato]: Por coletor; quem não tem mediana válida na janela fica de fora.
        """
        resultado = {}
        for coletor, sinal in self.sinais.get(pulseira, {}).items():
            if not self.disponivel(coletor):
                continue
            validas = [
                m for m in sinal.medianas
                if m.recebida_em >= desde and m.amostras >= self.minimo_amostras
            ]
            if not validas:
                continue
            resultado[coletor] = Candidato(
                rssi_suavizado=fmean([m.rssi for m in validas], weights=[m.amostras for m in validas]),
                amostras=sum(m.amostras for m in validas),
                primeira_em=validas[0].recebida_em,
            )
        return resultado


    def __str__(self) -> str:
        """Retrato da visão do coordenador, em várias linhas, para o log.

        Legenda: `*` = coletor com a posse; `[indisponivel]` = coletor offline ou sem sinal de vida,
        que não entra em disputa.
        """
        online = ", ".join(sorted(self.coletores_online)) or "nenhum"
        linhas = [f"Visão dos coletores | online: [{online}] | mínimo de amostras: {self.minimo_amostras}"]

        agora = time.monotonic()
        coletores = sorted(self.coletores_online | self.coletores_offline | self.ultima_mensagem.keys())
        for coletor in coletores:
            if coletor in self.coletores_sem_sinal:
                presenca = "sem-sinal"
            elif coletor in self.coletores_online:
                presenca = "online"
            elif coletor in self.coletores_offline:
                presenca = "offline"
            else:
                presenca = "desconhecida"
            visto = self.ultima_mensagem.get(coletor)
            idade = "-" if visto is None else f"há {agora - visto:.1f}s"
            linhas.append(
                f"  Coletor {coletor:<20} | presença: {presenca:<12} | "
                f"pulseira: {self.pulseira_atendida.get(coletor) or '-'} | última mensagem: {idade}"
            )

        pulseiras = sorted(self.sinais.keys() | self.posse.keys())
        if not pulseiras:
            linhas.append("  (nenhuma pulseira conhecida)")

        for pulseira in pulseiras:
            dono = self.posse.get(pulseira)
            linhas.append(f"  Pulseira {pulseira} | posse: {dono or 'ninguém'}")
            sinais = self.sinais.get(pulseira, {})
            if not sinais:
                linhas.append("      (sem sinais)")
            for coletor, sinal in sorted(sinais.items()):
                marca = "*" if coletor == dono else " "
                aviso = "" if self.disponivel(coletor) else " [indisponivel]"
                linhas.append(f"    {marca} {coletor:<20} {sinal}{aviso}")

        return "\n".join(linhas)
