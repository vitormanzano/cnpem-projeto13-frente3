import asyncio
import logging
import math
import random
from dataclasses import dataclass

from bluetooth.domain.erros import BrokerIndisponivel
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from mqtt.estado_rssi import EstadoRssiColetor
from mqtt.frequencia_cardiaca import FrequenciaCardiacaColetor


PULSEIRA_SIMULADA = "00:00:00:00:00:01"
INTERVALO_PASSO   = 1.0  # s: uma FC e um RSSI por segundo, como a pulseira e o coletor reais
INTERVALO_CONEXAO = 0.1  # s entre as verificações da conexão dos clientes simulados ao broker

# Roteiro de um ciclo, em segundos desde o início dele. A cada ciclo a posse passa de um coletor ao outro.
DURACAO_CICLO  = 90
INICIO_DISPUTA = 30        # Até aqui o sinal do dono enfraquece e passa do limiar de conexao-fraca; então ele solta
FIM_DISPUTA    = 33        # 3 s com a posse liberada, como a janela de disputa (RF14)
QUEDA_BLE      = (55, 62)  # O novo dono perde a pulseira por 7 s e reconecta (RF05): mais de 5 s sem leitura
SEM_FC         = (75, 82)  # Conectado, mas 7 s sem frequência cardíaca (ex.: sem contato com a pele)

RSSI_INICIAL          = -58  # Ótimo
RSSI_FINAL            = -84  # Crítico, abaixo do limiar padrão de -78 dBm
RSSI_ESTAVEL          = -64  # Bom
RSSI_VARREDURA_ORIGEM = -84  # Na disputa, a origem quase não enxerga a pulseira...
RSSI_VARREDURA_NOVO   = -62  # ...e o outro coletor a enxerga bem
AMOSTRAS_VARREDURA    = 6
RUIDO_RSSI            = 1.5  # dB, desvio padrão
BPM_INICIAL           = 74.0


@dataclass(frozen=True)
class PassoRoteiro:
    """O que acontece num segundo do roteiro simulado."""
    dono: int | None     # Índice do coletor simulado com a posse; None durante a disputa
    conectado: bool      # O dono está conectado à pulseira
    rssi: float | None   # RSSI do dono, antes do ruído; None sem conexão
    com_fc: bool         # A pulseira envia frequência cardíaca


def roteiro(segundo: int) -> PassoRoteiro:
    """Passo do roteiro no `segundo` desde o início da simulação. Função pura, para os testes."""
    ciclo, t = divmod(segundo, DURACAO_CICLO)
    origem, destino = (0, 1) if ciclo % 2 == 0 else (1, 0)
    if t < INICIO_DISPUTA:
        rssi = RSSI_INICIAL + (RSSI_FINAL - RSSI_INICIAL) * t / INICIO_DISPUTA
        return PassoRoteiro(dono=origem, conectado=True, rssi=rssi, com_fc=True)
    if t < FIM_DISPUTA:
        return PassoRoteiro(dono=None, conectado=False, rssi=None, com_fc=False)
    if QUEDA_BLE[0] <= t < QUEDA_BLE[1]:
        return PassoRoteiro(dono=destino, conectado=False, rssi=None, com_fc=False)
    return PassoRoteiro(dono=destino, conectado=True, rssi=RSSI_ESTAVEL, com_fc=not SEM_FC[0] <= t < SEM_FC[1])


@dataclass(frozen=True)
class ColetorSimulado:
    """Publica com as mesmas classes do coletor real, então o painel recebe o formato de produção."""
    broker: ConexaoBroker
    estado_rssi: EstadoRssiColetor
    frequencia: FrequenciaCardiacaColetor

    @property
    def id(self) -> str:
        return self.broker.cliente_id


class SimuladorPulseira:
    """Modo simulado do painel (RF20): dois coletores e um coordenador falsos publicam no broker, sem pulseira.

    O roteiro repete a cada DURACAO_CICLO segundos: o sinal do dono enfraquece até crítico, ele solta
    a pulseira (handoff), a posse fica liberada durante a disputa, o outro coletor assume, perde a
    conexão por alguns segundos, reconecta e passa um tempo sem frequência cardíaca.

    O coordenador simulado publica a posse da pulseira simulada. Não rode junto do coordenador real,
    que deveria ser o único a publicar posse (RF14).
    """

    def __init__(
        self,
        coordenador: ConexaoBroker,
        coletores: list[ConexaoBroker],
        pulseira: str = PULSEIRA_SIMULADA,
        semente: int | None = None,
    ) -> None:
        """
        Args:
            coordenador (ConexaoBroker): Sessão de quem publica a posse.
            coletores (list[ConexaoBroker]): Duas sessões, uma por coletor simulado; o client id é o id do coletor.
            pulseira (str): Endereço da pulseira simulada.
            semente (int | None): Semente do ruído, para repetir a mesma simulação.

        Raises:
            ValueError: Se não forem dois coletores, ou se o buffer de FC configurado for inválido.
        """
        if len(coletores) != 2:
            raise ValueError("O roteiro simulado usa exatamente dois coletores.")
        self.coordenador = coordenador
        self.pulseira = pulseira
        self.coletores = [
            ColetorSimulado(broker, EstadoRssiColetor(broker), FrequenciaCardiacaColetor(broker, pulseira))
            for broker in coletores
        ]
        self._aleatorio = random.Random(semente)
        self._dono: int | None = None
        self._ultimo_dono: int | None = None
        self._conectado = False
        self._bpm = BPM_INICIAL
        # A posse é retida: republicada a cada (re)conexão do coordenador simulado, como a última vontade
        self.coordenador.adicionar_ao_conectar(self._publicar_posse)


    @property
    def brokers(self) -> list[ConexaoBroker]:
        return [self.coordenador, *(coletor.broker for coletor in self.coletores)]


    async def passo(self, segundo: int) -> None:
        """Executa um segundo do roteiro: troca a posse se for o caso, gera as leituras e publica."""
        atual = roteiro(segundo)
        if atual.dono != self._dono:
            await self._trocar_posse(atual.dono)

        if atual.dono is None:
            self._registrar_varredura()
        else:
            self._registrar_dono(self.coletores[atual.dono], atual, segundo)
        await self._publicar()


    async def executar(self) -> None:
        """Conecta os clientes simulados e segue o roteiro, um passo por segundo, até ser cancelado."""
        tarefas = {asyncio.create_task(broker.manter_conexao()) for broker in self.brokers}
        try:
            # Antes da primeira posse: sem broker ela falharia e só sairia na reconexão
            while not all(broker.conectado for broker in self.brokers):
                await asyncio.sleep(INTERVALO_CONEXAO)
            logging.info(
                f"Simulador | Pulseira: {self.pulseira} | Coletores: {', '.join(c.id for c in self.coletores)} | "
                f"Ciclo: {DURACAO_CICLO} s"
            )
            segundo = 0
            while True:
                await self.passo(segundo)
                segundo += 1
                await asyncio.sleep(INTERVALO_PASSO)
        finally:
            # Deixa a posse retida da pulseira simulada como liberada
            if self._dono is not None and self.coordenador.conectado:
                self._dono = None
                await self._publicar_posse()
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)


    async def _trocar_posse(self, novo: int | None) -> None:
        if self._dono is not None:
            # Como no Coletor._liberar: o estado desconectado e a FC guardada saem antes da troca (RF06, RF17)
            origem = self.coletores[self._dono]
            if self._conectado:
                origem.estado_rssi.registrar(LeituraRSSI(self.pulseira, EstadoConexao.DESCONECTADO))
            await origem.estado_rssi.publicar_pendentes()
            if origem.broker.conectado:
                await origem.frequencia.publicar_pendentes()
            origem.frequencia.perder_posse()
            self._ultimo_dono = self._dono

        self._dono = novo
        self._conectado = False
        if novo is not None:
            self.coletores[novo].frequencia.receber_posse()
        logging.info(
            f"Simulador | Pulseira: {self.pulseira} | "
            f"Posse: {'liberada' if novo is None else self.coletores[novo].id}"
        )
        await self._publicar_posse()


    def _registrar_varredura(self) -> None:
        """Na disputa, os dois coletores varrem e publicam a mediana dos anúncios (RF18)."""
        for indice, coletor in enumerate(self.coletores):
            base = RSSI_VARREDURA_ORIGEM if indice == self._ultimo_dono else RSSI_VARREDURA_NOVO
            coletor.estado_rssi.registrar(LeituraRSSI(
                self.pulseira, EstadoConexao.DESCONECTADO, rssi=self._com_ruido(base), amostras=AMOSTRAS_VARREDURA,
            ))


    def _registrar_dono(self, coletor: ColetorSimulado, passo: PassoRoteiro, segundo: int) -> None:
        # Idempotente. Repõe a confirmação de posse que a FrequenciaCardiacaColetor suspende quando o broker
        # cai; no coletor real, quem a repõe é a posse retida que chega na reconexão
        coletor.frequencia.receber_posse()
        if passo.conectado and passo.rssi is not None:
            coletor.estado_rssi.registrar(LeituraRSSI(
                self.pulseira, EstadoConexao.CONECTADO, rssi=self._com_ruido(passo.rssi),
            ))
            if passo.com_fc:
                coletor.frequencia.registrar(LeituraFrequenciaCardiaca(bpm=self._proximo_bpm(segundo)))
        elif self._conectado:
            # Como a ConexaoPulseira: o desconectado sai uma vez, na mudança, e depois o coletor tenta reconectar
            coletor.estado_rssi.registrar(LeituraRSSI(self.pulseira, EstadoConexao.DESCONECTADO))
        self._conectado = passo.conectado


    async def _publicar(self) -> None:
        for coletor in self.coletores:
            # Sem broker, o estado é descartado (a reconexão republica o último) e a FC fica guardada (RF17)
            await coletor.estado_rssi.publicar_pendentes()
            if coletor.broker.conectado:
                await coletor.frequencia.publicar_pendentes()


    async def _publicar_posse(self) -> None:
        dono = None if self._dono is None else self.coletores[self._dono].id
        mensagem = Mensagem(
            tipo=TipoMensagem.POSSE,
            origem=self.coordenador.cliente_id,
            conteudo={"pulseira": self.pulseira, "coletor": dono},
        )
        try:
            await self.coordenador.publicar(Topicos.estado_posse(self.pulseira), mensagem)
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Simulador | Pulseira: {self.pulseira} | "
                f"Posse não publicada, sai na reconexão: {e}"
            )


    def _com_ruido(self, rssi: float) -> int:
        return round(rssi + self._aleatorio.gauss(0, RUIDO_RSSI))


    def _proximo_bpm(self, segundo: int) -> int:
        alvo = BPM_INICIAL + 10 * math.sin(segundo / 25)
        self._bpm += (alvo - self._bpm) * 0.15 + self._aleatorio.gauss(0, 0.8)
        return round(self._bpm)
