import asyncio
import os
import logging
from typing import TYPE_CHECKING

import aiomqtt
from gateway.bluetooth.domain.erros import BrokerIndisponivel
from gateway.mqtt.conexao import ConexaoBroker
from gateway.mqtt.domain.models import EstadoPresenca, Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos


INTERVALO_SINAL_DE_VIDA_PADRAO = 5.0


if TYPE_CHECKING:
    from mqtt.conexao import ConexaoBroker


def ler_intervalo_sinal_de_vida(intervalo: float | None = None) -> float:
    """Resolve o intervalo de sinal de vida.

    Se omitido, é lido da variável de ambiente PRESENCA_INTERVALO (padrão de 5 s).

    Raises: 
        ValueError: Se o intervalo não for positivo ou a variável de ambiente não for um número.
    """
    if intervalo is None:
        intervalo = float(os.getenv("PRESENCA_INTERVALO_COLETOR", INTERVALO_SINAL_DE_VIDA_PADRAO))

    if intervalo <= 0:
        raise ValueError("PRESENCA_INTERVALO_COLETOR deve ser maior que zero.")
    return intervalo


class PresencaColetor:
    """Anuncia ao coordenador, pelo broker,  se o coletor está online ou offline."""

    def __init__(self, broker: "ConexaoBroker", intervalo: float | None = None) -> None:
        """Registra no broker a última vontade offline e o anúncio online a cada (re)conexão.

        Deve ser criada antes de iniciar `broker.manter_conexao()`, para que a última
        vontade já esteja registrada na primeira conexão.

        Args:
            broker (ConexaoBroker): Sessão MQTT do coletor.
            intervalo (float | None): Segundos entre os sinais de vida. Se omitido, é lido 
                de PRESENCA_INTERVALO (padrão de 5 s).

        Raises:
            ValueError: Se o intervalo for válido.
        """
        self.broker = broker
        self.intervalo = ler_intervalo_sinal_de_vida(intervalo)
        self.pulseira: str | None = None
        self._encerrada = False

        self.broker.ultima_vontade = self._ultima_vontade()
        self.broker.ao_conectar = self.anunciar_online


    @property
    def topico(self) -> str:
        return Topicos.presenca_coletor(self.broker.cliente_id)

    async def anunciar_online(self) -> None:
        """Publica a presença online (retida). Chamada a cada (re)conexão ao broker."""
        if self._encerrada:
            return

        if await self._publicar(EstadoPresenca.ONLINE):
            logging.info(f"Presença: {EstadoPresenca.ONLINE.value} | Coletor: {self.broker.cliente_id} ")


    async def anunciar_offline(self) -> None:
        """Publica a presença offline (retida) no encerramento controlado.

        Deve ser chamado antes de desconectar do broker: numa desconexão limpa o broker
        descarta a última vontade, então este é o único aviso de offline. Depois dele, 
        o sinal de vida para e as reconexões não anunciam mais online.
        """
        self.encerrada = True
        if await self._publicar(EstadoPresenca.OFFLINE):
            logging.info(f"Presença: {EstadoPresenca.OFFLINE.value} | Coletor: {self.broker.cliente_id} ")


    async def  atualiar_pulseira(self, pulseira: str | None = None) -> None:
        """Atualiza a pulseira atendida e publica a presença na hora.

        Args: 
            pulseira (str | None): Endereço da pulseira de que o coletor tem posse,
                Ou None quando não atende nenhuma.
        """
        if pulseira == self.pulseira: 
            return 

        self.pulseira = pulseira
        logging.info(f"Presença | Coletor: {self.broker.cliente_id} | Pulseira: {pulseira or 'Nenhuma' } ")
        if not self._encerrada:
            """ Se falhar, o próximo sinal de vida manda a pulseira atualizada."""
            await self._publicar(EstadoPresenca.ONLINE)


    async def manter_sinal_de_vida(self) -> None:
        """Publica  a presença a cada `intervalo` segundos, até o encerramento ou cancelamento.

        Sem conexão com o broker o sinal é pulado; a falha ja é registrada pela ConexaoBroker
        e a presença é republicada na conexão.
        """
        while not self._encerrada:
            await asyncio.sleep(self.intervalo)
            if self._encerrada or not self.broker.conectado:
                continue

            if await self._publicar(EstadoPresenca.ONLINE):
                logging.debug(f"Sinal de vida | Coletor: {self.broker.cliente_id} ")


    def mensagem(self, estado: EstadoPresenca) -> Mensagem:
        """Monta a mensagem de presença. O identificador do coletor (origem) e o timestamp vão no envelope."""
        return Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": estado.value,
                "pulseira": self.pulseira
            },
        )

    
    async def _publicar(self, estado: EstadoPresenca) -> bool:
        """Publica a presença. Devolve False se o broker estiver indisponível, sem lançar erro."""
        try:
            await self.broker.publicar(self.topico, self.mensagem(estado))
            return True
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Coletor: {self.broker.cliente_id} | "
                f"Não foi possível publicar a presença {estado.value}: {e}"
            )
            return False
    

    def _ultima_vontade(self) -> aiomqtt.Will:
        """Presença offline publicada pelo broker se o coletor cair sem avisar.

        O timestamp é o do registro, não o da queda: para saber quando o coletor caiu,
        o coordenador deve usar o horário de recebimento.
        """
        mensagem = Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": EstadoPresenca.OFFLINE.value,
                "pulseira": None
            },
        )
        config = mensagem.config_entrega
        return aiomqtt.Will(
                topic=self.topico,
                payload=mensagem.para_json(),
                qos=config.qos,
                retain=config.retida,
        )
        
