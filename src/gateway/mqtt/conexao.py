import asyncio
import inspect
import logging
import os
import socket
from collections.abc import Awaitable, Callable

import aiomqtt
from bluetooth.domain.erros import BrokerIndisponivel
from mqtt.domain.models import Mensagem


HOST_PADRAO = "localhost"
PORTA_PADRAO = 1883
INTERVALO_RECONEXAO_PADRAO = 5.0
KEEPALIVE_PADRAO = 10

TratadorMensagem = Callable[[str, Mensagem], Awaitable[None] | None]
TratadorConexao  = Callable[[], Awaitable[None] | None]


class ConexaoBroker:
    """Sessão MQTT entre o coletor e o broker, mantida enquanto o coletor estiver ativo."""

    def __init__(
        self,
        cliente_id: str | None = None,
        host: str | None = None,
        porta: int | None = None,
        usuario: str | None = None,
        senha: str | None = None,
        intervalo_reconexao: float | None = None,
        ao_receber: TratadorMensagem | None = None,
        ultima_vontade: aiomqtt.Will | None = None,
        ao_conectar: TratadorConexao | None = None
    ) -> None:
        """
        Args:
            cliente_id (str | None): Identificador único do coletor, usado como client id. Se omitido,
                é lido da variável de ambiente COLETOR_ID (padrão: hostname da máquina).
            host (str | None): Endereço do broker. Se omitido, é lido de BROKER_HOST (padrão: localhost).
            porta (int | None): Porta do broker. Se omitida, é lida de BROKER_PORTA (padrão: 1883).
            usuario (str | None): Usuário do broker. Se omitido, é lido de BROKER_USUARIO.
            senha (str | None): Senha do broker. Se omitida, é lida de BROKER_SENHA.
            intervalo_reconexao (float | None): Tempo entre tentativas de reconexão em segundos. Se omitido,
                é lido de BROKER_INTERVALO_RECONEXAO (padrão de 5 s).
            ao_receber (TratadorMensagem | None): Função chamada com o tópico e a mensagem a cada
                mensagem válida recebida nos tópicos assinados.
            ultima_vontade (aiomqtt.Will | None): Mensagem publicada pelo broker se o coletor cair sem avisar.
            ao_conectar (TratadorConexao | None): Função chamada a cada (re)conexão ao broker, 
                depois de refazer as assinaturas.
        """
        self.cliente_id  = cliente_id
        self.host = host or os.getenv("BROKER_HOST", HOST_PADRAO)
        self.porta = porta or int(os.getenv("BROKER_PORTA", PORTA_PADRAO))
        self.usuario = usuario or os.getenv("BROKER_USUARIO") or None
        self.senha = senha or os.getenv("BROKER_SENHA") or None
        self.intervalo_reconexao = intervalo_reconexao or float(
            os.getenv("BROKER_INTERVALO_RECONEXAO", INTERVALO_RECONEXAO_PADRAO)
        )
        self.ao_receber = ao_receber
        self.ultima_vontade = ultima_vontade
        self.ao_conectar = ao_conectar
        self._client: aiomqtt.Client | None = None
        self._assinaturas: dict[str, int] = {}


    @property
    def conectado(self) -> bool:
        return self._client is not None


    @property
    def endereco(self) -> str:
        return f"{self.host}:{self.porta}"


    async def assinar(self, topico: str, qos: int = 1) -> None:
        """Assina um tópico. A assinatura é refeita automaticamente a cada reconexão.

        Args:
            topico (str): Tópico ou filtro de tópicos (aceita os curingas + e #).
            qos (int): Nível de entrega da assinatura.
        """
        self._assinaturas[topico] = qos
        if self._client is None:
            return

        try:
            await self._client.subscribe(topico, qos=qos)
            logging.info(f"Tópico assinado: {topico} (QoS {qos})")
        except aiomqtt.MqttError as e:
            logging.warning(f"Falha ao assinar {topico}, será refeita na reconexão: {e}")


    async def cancelar_assinatura(self, topico: str) -> None:
        """Cancela a assinatura de um tópico, inclusive para as próximas reconexões."""
        self._assinaturas.pop(topico, None)
        if self._client is None:
            return

        try:
            await self._client.unsubscribe(topico)
        except aiomqtt.MqttError as e:
            logging.warning(f"Falha ao cancelar assinatura de {topico}: {e}")


    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        """Publica uma mensagem com o QoS e a retenção definidos para o seu tipo.

        Raises:
            BrokerIndisponivel: Se o coletor não estiver conectado ao broker.
        """
        if self._client is None:
            raise BrokerIndisponivel(f"Coletor {self.cliente_id } não está conectado ao broker {self.endereco}")

        config = mensagem.config_entrega
        try:
            await self._client.publish(topico, mensagem.para_json(), qos=config.qos, retain=config.retida)
        except aiomqtt.MqttError as e:
            raise BrokerIndisponivel(str(e)) from e


    async def manter_conexao(self) -> None:
        """Conecta ao broker e reconecta sempre que a conexão cair, até a tarefa ser cancelada.

        Falhas de conexão são registradas em log e nunca encerram o coletor.
        """
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self.host,
                    port=self.porta,
                    identifier=self.cliente_id ,
                    username=self.usuario,
                    password=self.senha,
                    will=self.ultima_vontade,
                    keepalive=KEEPALIVE_PADRAO,
                ) as client:
                    self._client = client
                    logging.info(f"Conectado ao broker {self.endereco} | Coletor {self.cliente_id }")
                    await self._refazer_assinaturas()
                    await self._executar_ao_conectar()

                    async for mensagem in client.messages:
                        await self._tratar_mensagem(mensagem)

            except aiomqtt.MqttError as e:
                motivo = "conexão perdida" if self._client is not None else "falha ao conectar"
                logging.error(
                    f"Erro: {BrokerIndisponivel.codigo} | Broker {self.endereco}: {motivo} ({e}) | "
                    f"nova tentativa em {self.intervalo_reconexao:g}s"
                )
            finally:
                self._client = None

            await asyncio.sleep(self.intervalo_reconexao)


    async def _refazer_assinaturas(self) -> None:
        for topico, qos in self._assinaturas.items():
            await self._client.subscribe(topico, qos=qos)
            logging.info(f"Tópico assinado: {topico} (QoS {qos})")


    async def _tratar_mensagem(self, mensagem: aiomqtt.Message) -> None:
        conteudo = Mensagem.de_json(mensagem.payload)
        if conteudo is None or self.ao_receber is None:
            return

        try:
            resultado = self.ao_receber(mensagem.topic.value, conteudo)
            if inspect.isawaitable(resultado):
                await resultado
        except Exception as e:
            # Um erro no tratamento de uma mensagem não pode derrubar a sessão com o broker
            logging.exception(f"Erro ao tratar mensagem do tópico {mensagem.topic.value} [{type(e).__name__}]: {e}")


    async def _executar_ao_conectar(self) -> None:
        if self.ao_conectar is None:
            return

        try:
            resultado = self.ao_conectar()
            if inspect.isawaitable(resultado):
                await resultado

        except  Exception as e:
            # Uma falha no gancho não pode derrubar a sessão com o broker
            logging.exception(f"Erro ao executar ao_conectar [{type(e).__name__}]: {e}")
