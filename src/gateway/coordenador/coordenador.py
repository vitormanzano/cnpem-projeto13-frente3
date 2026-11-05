import asyncio
import os
import logging
from typing import TYPE_CHECKING

import aiomqtt
from bluetooth.domain.erros import BrokerIndisponivel
from gateway.bluetooth.domain.models import EstadoConexao
from gateway.coordenador.visao import VisaoColetores
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
        self.intervalo: float      = ler_intervalo_sinal_de_vida(intervalo)
        self.visao                 = VisaoColetores()
        self._encerrada            = False
        
        self.broker.ultima_vontade = self._ultima_vontade()
        self.broker.ao_conectar    = self.anunciar_online
        self.broker.ao_receber     = self.ao_receber


    async def assinar_topicos(self) -> None:
        """Assina, presença, sinal e posse de todos os coletores e pulseiras.

        Pode ser chamado antes de conectar: a ConexaoBroker refaz as assinaturas de cada
        (re)conexão, e o broker entrega na hora as mensagens retidas de presença e posse.
        """
        await self.broker.assinar(Topicos.presenca_coletor("+"))
        await self.broker.assinar(Topicos.estado_rssi("+", "+"), qos=0)
        await self.broker.assinar(Topicos.estado_posse("+"))

    
    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        """Encaminha cada mensagem pelo tipo; o tópico só é usado para extrair os ids."""
        match mensagem.tipo:
            case TipoMensagem.PRESENCA:
                self._tratar_presenca(topico, mensagem)
            case TipoMensagem.ESTADO_PULSEIRA:
                self._tratar_sinal(topico, mensagem)
            case TipoMensagem.POSSE:
                self._tratar_posse(topico, mensagem)


    def _tratar_presenca(self, topico: str, mensagem: Mensagem) -> None:
        coletor = Topicos.extrair_presenca_coletor(topico)
        if coletor is None:
            return

        online = mensagem.conteudo.get("estado") == EstadoPresenca.ONLINE.value
        # Só registra mudança; o sinal de vida a cada 5 s não deve poluir o log
        if self.visao.atualizar_presenca(coletor, online):
            estado = EstadoPresenca.ONLINE if online else EstadoPresenca.OFFLINE
            logging.info(f"Presença | Coletor: {coletor} | {estado.value}")


    def _tratar_sinal(self, topico: str, mensagem: Mensagem) -> None:
        ids = Topicos.extrair_estado_rssi(topico)
        if ids is None: 
            return
        coletor, pulseira = ids

        if mensagem.origem != coletor:
            logging.warning(f"Sinal descartado | {topico} | origem {mensagem.origem} não é o coletor do tópico")
            return

        try:
            estado = EstadoConexao(mensagem.conteudo["estado"])
            rssi = mensagem.conteudo.get("rssi")
            rssi = None if rssi is None else int(rssi)
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"Sinal descartado | {topico} | conteúdo inválido [{type(e).__name__}]: {e}")
            return

        sinal = self.visao.registrar_sinal(pulseira, coletor, estado, rssi)
        logging.debug(
            f"Sinal | Pulseira: {pulseira} | Coletor: {coletor} | {estado.value} | "
            f"RSSI: {rssi} | suavizado: {sinal.rssi_suavizado}"
        )


    def _tratar_posse(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_estado_posse(topico)
        if pulseira is None:
            return
        self.visao.atualizar_posse(pulseira, mensagem.conteudo.get("coletor"))

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
            await self.broker.publicar(Topicos.presenca_coordenador(), self.mensagem(estado))
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
                topic=Topicos.presenca_coordenador(),
                payload=mensagem.para_json(),
                qos=config.qos,
                retain=config.retida,
        )

