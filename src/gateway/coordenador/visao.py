from collections import deque
from dataclasses import dataclass
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
    amostras: deque[int]


    @property
    def rssi(self) -> int | None:
        """Último RSSI recebido."""
        return self.amostras[-1] if self.amostras else None


    @property
    def rssi_suavizada(self) -> float | None:
        return fmean(self.amostras) if self.amostras else None


    def __str__(self) -> str:
        rssi = "-" if self.rssi is None else f"{self.rssi} dBm"
        media = "-" if self.rssi_suavizada is None else f"{self.rssi_suavizada:.1f} dBm"
        idade = time.monotonic() - self.recebido_em
        return (
            f"{self.estado.value:<14} | rssi: {rssi:>8} | média: {media:>10} | "
            f"há {idade:4.1f}s | amostras: {list(self.amostras)}"
        )


class VisaoColetores:
    """Estado da rede montado a partir das mensagens do broker: quem está online,
    quem enxerga cada pulseira (e com que sinal) e quem tem a posse."""

    def __init__(
        self,
        tamanho_janela: int = TAMANHO_JANELA_PADRAO,
        validade: float = VALIDADE_SINAL_PADRAO,
    ) -> None:
        self.tamanho_janela = tamanho_janela
        self.validade = validade
        self.coletores_online: set[str] = set()
        self.sinais: dict[str, dict[str, SinalColetor]] = {} # pulseira -> coletor -> sinal
        self.posse: dict[str, str | None] = {} # pulseira -> coletor responsável


    def atualizar_presenca(self, coletor: str, online: bool) -> bool:
        """Atualiza a presença do coletor. Retorna true se o estado mudou"""
        if online: 
            mudou = coletor not in self.coletores_online
            self.coletores_online.add(coletor)
            return mudou

        mudou = coletor in self.coletores_online
        self.coletores_online.discard(coletor)
        # Leituras antigas de um coletor offline não podem vencer uma disputa.
        for sinais in self.sinais.values():
            sinais.pop(coletor, None)
        return mudou


    def registrar_sinal(self, pulseira: str, coletor: str, estado: EstadoConexao, rssi: int | None) -> SinalColetor:
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
            sinal.amostras.append(rssi)
        return sinal


    def atualizar_posse(self, pulseira: str, coletor: str | None) -> None:
        self.posse[pulseira] = coletor


    def sinais_validos(self, pulseira) -> dict[str, SinalColetor]:
        """Sinais recentes, de coletores online, que de fato enxergam a pulseira.

        O filtro é feito na leitura, e não no registro, porque o MQTT só garante ordem
        dentro de um mesmo tópico: O RSSI de um coletor pode chegar antes da presença dele.
        """
        agora = time.monotonic()
        resultado = {}
        for coletor, sinal in self.sinais.get(pulseira, {}).items():
            if (coletor in self.coletores_online
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

        Legenda: `*` = coletor com a posse; `[offline]` = coletor fora de `coletores_online`;
        `[expirado]` = leitura mais velha que `validade`. Sinais marcados não entram em disputa.
        """
        online = ", ".join(sorted(self.coletores_online)) or "nenhum"
        linhas = [f"Visão dos coletores | online: [{online}] | janela: {self.tamanho_janela} | validade: {self.validade:g}s"]

        pulseiras = sorted(self.sinais.keys() | self.posse.keys())
        if not pulseiras:
            linhas.append("  (nenhuma pulseira conhecida)")

        agora = time.monotonic()
        for pulseira in pulseiras:
            dono = self.posse.get(pulseira)
            linhas.append(f"  Pulseira {pulseira} | posse: {dono or 'ninguém'}")
            sinais = self.sinais.get(pulseira, {})
            if not sinais:
                linhas.append("      (sem sinais)")
            for coletor, sinal in sorted(sinais.items()):
                marca = "*" if coletor == dono else " "
                avisos = []
                if coletor not in self.coletores_online:
                    avisos.append("[offline]")
                if agora - sinal.recebido_em > self.validade:
                    avisos.append("[expirado]")
                linhas.append(f"    {marca} {coletor:<20} {sinal} {' '.join(avisos)}".rstrip())

        return "\n".join(linhas)
