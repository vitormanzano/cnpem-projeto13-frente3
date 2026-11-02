import asyncio
import os
import logging
from typing import TYPE_CHECKING

import aiomqtt
from bluetooth.domain.erros import BrokerIndisponivel
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import EstadoPresenca, Mensagem, TipoMensagem
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
        intervalo = float(os.getenv("PRESENCA_INTERVALO_COORDENADOR", INTERVALO_SINAL_DE_VIDA_PADRAO))

    if intervalo <= 0:
        raise ValueError("PRESENCA_INTERVALO_COLETOR deve ser maior que zero.")
    return intervalo


class Coordenador:
    """Responsável por gerenciar as conexões entre coletores e pulseiras."""

    def __init__(self, broker: "ConexaoBroker", intervalo: float | None = None) -> None:
        """Registra sua presença e coordena os coletores.
            Args:
                broker (ConexaoBroker): Sessão MQTT do coletor.
                intervalo (float | None): Segundos entre os sinais de vida. Se omitido, é lido 
                    de PRESENCA_INTERVALO (padrão de 5 s).

            Raises:
                ValueError: Se o intervalo for válido.
        """
        self.broker: ConexaoBroker = broker
        self.intervalo: float = ler_intervalo_sinal_de_vida(intervalo)
        self._encerrada = False
        
        self.broker.ultima_vontade = self._ultima_vontade()
        self.broker.ao_conectar = self.anunciar_online


    @property
    def topicos(self) -> list[str]:
        return [Topicos.presenca_coordenador()]


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
        """Publica  a presença a cada `intervalo` segundos, até o encerramento ou cancelamento.

        Sem conexão com o broker o sinal é pulado; a falha ja é registrada pela ConexaoBroker
        e a presença é republicada na conexão.
        """
        while not self._encerrada:
            await asyncio.sleep(self.intervalo)
            if self._encerrada or not self.broker.conectado:
                continue

            if await self._publicar_presenca(EstadoPresenca.ONLINE):
                logging.debug(f"Sinal de vida | Coordenador")


    async def _publicar_presenca(self, estado: EstadoPresenca) -> bool:
        """Publica a presença. Devolve False se o broker estiver indisponível, sem lançar erro."""
        try:
            await self.broker.publicar(self.topicos[0], self.mensagem(estado))
            return True
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Coletor: {self.broker.cliente_id } | "
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
                topic=self.topicos[0],
                payload=mensagem.para_json(),
                qos=config.qos,
                retain=config.retida,
        )
