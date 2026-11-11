from collections import deque
from dataclasses import dataclass
from datetime import datetime
import time
from statistics import fmean

from bluetooth.domain.models import EstadoConexao


TAMANHO_JANELA_PADRAO = 5
VALIDADE_SINAL_PADRAO = 3.0


@dataclass
class SinalColetor:
    """O que o coordenador sabe do sinal de um coletor para uma pulseira"""
    estado: EstadoConexao
    recebido_em: float
    amostras: deque[tuple[int, int]] # (Mediana, quantidade de anúncios)


    @property
    def rssi(self) -> int | None:
        """Último RSSI recebido."""
        return self.amostras[-1][0] if self.amostras else None


    @property
    def rssi_suavizada(self) -> float | None:
        """Média das medianas, ponderada pela qunatidade de anúncios de cada uma."""
        if not self.amostras:
            return None
        medianas, quantidade = zip(*self.amostras)
        return fmean(medianas, weights=quantidade)

     
    @property
    def total_amostras(self) -> int:
        return sum(n for _, n in self.amostras)


    def __str__(self) -> str:
        rssi = "-" if self.rssi is None else f"{self.rssi} dBm"
        media = "-" if self.rssi_suavizada is None else f"{self.rssi_suavizada:.1f} dBm"
        idade = time.monotonic() - self.recebido_em
        return (
            f"{self.estado.value:<14} | rssi: {rssi:>8} | média: {media:>10} | "
            f"há {idade:4.1f}s | amostras: {list(self.amostras)}"
        )


class VisaoColetores:
    """Estado da rede montado a partir das mensagens do broker: quem está disponível,
    quem enxerga cada pulseira (e com que sinal) e quem tem a posse (RF13)."""

    def __init__(
        self,
        tamanho_janela: int = TAMANHO_JANELA_PADRAO,
        validade: float = VALIDADE_SINAL_PADRAO,
    ) -> None:
        self.tamanho_janela = tamanho_janela
        self.validade = validade
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


    def registrar_sinal(self, pulseira: str, coletor: str, estado: EstadoConexao, rssi: int | None, amostras: int = 1) -> SinalColetor:
        agora = time.monotonic()
        sinais = self.sinais.setdefault(pulseira, {})
        sinal = sinais.get(coletor)
        if sinal is None:
            sinal = SinalColetor(estado, agora, deque(maxlen=self.tamanho_janela))
            sinais[coletor] = sinal

        sinal.estado = estado
        sinal.recebido_em = agora
        if rssi is None:
            """Significa nao-encontrado, as amostras anteriores não representam mais o sinal"""
            sinal.amostras.clear()
        else:
            sinal.amostras.append((rssi, amostras))
        return sinal


    def atualizar_posse(self, pulseira: str, coletor: str | None) -> None:
        self.posse[pulseira] = coletor


    def pulseiras_de(self, coletor: str) -> list[str]:
        """Pulseiras cuja posse está com o coletor."""
        return [pulseira for pulseira, dono in self.posse.items() if dono == coletor]


    def sinais_validos(self, pulseira) -> dict[str, SinalColetor]:
        """Sinais recentes, de coletores disponíveis, que de fato enxergam a pulseira.

        O filtro é feito na leitura, e não no registro, porque o MQTT só garante ordem
        dentro de um mesmo tópico: O RSSI de um coletor pode chegar antes da presença dele.
        """
        agora = time.monotonic()
        resultado = {}
        for coletor, sinal in self.sinais.get(pulseira, {}).items():
            if (self.disponivel(coletor)
                and agora - sinal.recebido_em <= self.validade
                and sinal.rssi is not None
            ):
                resultado[coletor] = sinal
        return resultado


    def limpar_amostras(self, pulseira: str) -> None:
        """A disputa de um handoff só deve comparar leituras feitas depois da liberação."""
        for sinal in self.sinais.get(pulseira, {}).values():
            sinal.amostras.clear()


    def __str__(self) -> str:
        """Retrato da visão do coordenador, em várias linhas, para o log.

        Legenda: `*` = coletor com a posse; `[indisponivel]` = coletor offline ou sem sinal de vida;
        `[expirado]` = leitura mais velha que `validade`. Sinais marcados não entram em disputa.
        """
        online = ", ".join(sorted(self.coletores_online)) or "nenhum"
        linhas = [f"Visão dos coletores | online: [{online}] | janela: {self.tamanho_janela} | validade: {self.validade:g}s"]

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
                avisos = []
                if not self.disponivel(coletor):
                    avisos.append("[indisponivel]")
                if agora - sinal.recebido_em > self.validade:
                    avisos.append("[expirado]")
                linhas.append(f"    {marca} {coletor:<20} {sinal} {' '.join(avisos)}".rstrip())

        return "\n".join(linhas)
