import asyncio
import logging
import os

from bluetooth.domain.erros import BrokerIndisponivel
from bluetooth.domain.models import LeituraRSSI
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos


TAMANHO_FILA_PADRAO = 100

class EstadoRssiColetor:
    """Anuncia ao coordenador, pelo broker, o estado de conexão e RSSI da pulseira que está conectado."""

    def __init__(self, broker: ConexaoBroker, tamanho_fila: int | None = None) -> None:
        """Deve ser criada depois da PresencaColetor, para que a reconexão anuncie online antes 

        Args:
            broker (ConexaoBroker): Sessão MQTT do coletor.
            tamanho_file (int | None): Máximo de leituras aguardando publicação. Se omitido, é lido de ESTADO_RSSI_TAMANHO_FILA (padrão de 100).
        """
        if tamanho_fila is None:
            tamanho_fila = int(os.getenv("ESTADO_RSSI_TAMANHO_FILA", TAMANHO_FILA_PADRAO))

        self.broker = broker
        self._fila:asyncio.Queue[LeituraRSSI] = asyncio.Queue(maxsize=tamanho_fila)
        self._ultima: LeituraRSSI | None = None
        self.broker.adicionar_ao_conectar(self.republicar_ultima)


    def registrar(self, leitura: LeituraRSSI) -> None:
        """Enfileira uma leitura da conexão ativa. Síncrono e nunca bloqueia: é o callback da ConexaoPulseira"""
        self._ultima = leitura
        if self._fila.full():
            self._fila.get_nowait() # Descarta a mais antiga
        self._fila.put_nowait(leitura)


    async def manter_publicacao(self) -> None:
        """Publica as leituras na ordem em que chegam, até a tarefa ser cancelada."""
        while True:
            leitura = await self._fila.get()
            if not self.broker.conectado:
                # A reconexão republica o último estado.
                continue
            await self._publicar(leitura)


    async def republicar_ultima(self) -> None:
        """Republica o último estado da pulseira. Chaamada a cada (re)conexão ao broker"""
        if self._ultima is not None:
            await self._publicar(self._ultima)
    

    def mensagem(self, leitura: LeituraRSSI) -> Mensagem:
        """Coletor (origem) e timestamp da leitura vão no envelope"""
        return Mensagem(
            tipo=TipoMensagem.ESTADO_PULSEIRA,
            origem=self.broker.cliente_id,
            timestamp=leitura.timestamp,
            conteudo={
                "pulseira": leitura.endereco,
                "estado": leitura.estado.value,
                "rssi": leitura.rssi,
                "amostras": leitura.amostras
            },
        )


    async def _publicar(self, leitura: LeituraRSSI) -> bool:
        topico = Topicos.estado_rssi(self.broker.cliente_id, leitura.endereco)
        try:
            await self.broker.publicar(topico, self.mensagem(leitura))
            return True
        except BrokerIndisponivel as e:
              logging.warning(
                  f"Erro: {BrokerIndisponivel.codigo} | Coletor: {self.broker.cliente_id} | "
                  f"Pulseira: {leitura.endereco} | Estado/RSSI não publicado: {e}"
              )
              return False

