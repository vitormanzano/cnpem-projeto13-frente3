import asyncio
import inspect
import logging
import math
import os
from collections.abc import Awaitable, Callable

import aiomqtt
from bluetooth.domain.erros import BrokerIndisponivel
from mqtt.domain.models import Mensagem


HOST_PADRAO = "localhost"
PORTA_PADRAO = 1883
INTERVALO_RECONEXAO_PADRAO = 5.0
KEEPALIVE_PADRAO = 5.0  # O sinal de vida padrão (RF12); PresencaColetor e Coordenador trocam pelo seu intervalo

TratadorMensagem = Callable[[str, Mensagem], Awaitable[None] | None]
TratadorConexao  = Callable[[], Awaitable[None] | None]


def ler_porta_broker(porta: int | None = None) -> int:
    """Resolve a porta do broker (RF11).

    Se omitida, é lida de BROKER_PORTA (padrão: 1883).

    Raises:
        ValueError: Se a porta não for um inteiro entre 1 e 65535.
    """
    if porta is None:
        valor = os.getenv("BROKER_PORTA") or str(PORTA_PADRAO)
        try:
            porta = int(valor)
        except ValueError as e:
            raise ValueError(f"BROKER_PORTA deve ser um número inteiro, recebido {valor!r}.") from e

    if not 1 <= porta <= 65535:
        raise ValueError(f"BROKER_PORTA deve estar entre 1 e 65535, recebido {porta}.")
    return porta


def ler_intervalo_reconexao_broker(intervalo: float | None = None) -> float:
    """Resolve o tempo entre as tentativas de reconexão ao broker (RF11).

    Se omitido, é lido de BROKER_INTERVALO_RECONEXAO (padrão de 5 s).

    Raises:
        ValueError: Se o intervalo não for um número maior que zero.
    """
    if intervalo is None:
        valor = os.getenv("BROKER_INTERVALO_RECONEXAO") or str(INTERVALO_RECONEXAO_PADRAO)
        try:
            intervalo = float(valor)
        except ValueError as e:
            raise ValueError(f"BROKER_INTERVALO_RECONEXAO deve ser um número, recebido {valor!r}.") from e

    # Com zero ou negativo o asyncio.sleep não espera e a reconexão vira um laço de erros
    if intervalo <= 0:
        raise ValueError("BROKER_INTERVALO_RECONEXAO deve ser maior que zero.")
    return intervalo


class ConexaoBroker:
    """Sessão MQTT entre o coletor e o broker, mantida enquanto o coletor estiver ativo."""

    def __init__(
        self,
        cliente_id: str ,
        host: str | None = None,
        porta: int | None = None,
        usuario: str | None = None,
        senha: str | None = None,
        intervalo_reconexao: float | None = None,
        ao_receber: TratadorMensagem | None = None,
        ultima_vontade: aiomqtt.Will | None = None,
        ao_conectar: TratadorConexao | None = None,
        keepalive: float | None = None,
        ao_desconectar: TratadorConexao | None = None,
    ) -> None:
        """
        Args:
            cliente_id (str | None): Identificador único do cliente, usado como client id.
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
            keepalive (float | None): Keepalive da sessão MQTT em segundos, igual ao intervalo do
                sinal de vida (RF11). Se omitido, usa 5 s; a PresencaColetor e o Coordenador o trocam
                pelo seu intervalo. Não tem variável própria para não divergir do sinal de vida.
            ao_desconectar (TratadorConexao | None): Função chamada quando uma sessão ativa cai.
                Não é chamada nas tentativas de conexão que falham nem no encerramento.

        Raises:
            ValueError: Se a porta ou o intervalo de reconexão forem inválidos.
        """
        self.cliente_id = cliente_id
        self.host = host or os.getenv("BROKER_HOST") or HOST_PADRAO
        self.porta = ler_porta_broker(porta)
        self.usuario = usuario or os.getenv("BROKER_USUARIO") or None
        self.senha = senha or os.getenv("BROKER_SENHA") or None
        self.intervalo_reconexao = ler_intervalo_reconexao_broker(intervalo_reconexao)
        self.keepalive = keepalive if keepalive is not None else KEEPALIVE_PADRAO
        self.ao_receber = ao_receber
        self.ultima_vontade = ultima_vontade
        self._ao_conectar: list[TratadorConexao] = [ao_conectar] if ao_conectar else []
        self._ao_desconectar: list[TratadorConexao] = [ao_desconectar] if ao_desconectar else []
        self._client: aiomqtt.Client | None = None
        self._assinaturas: dict[str, int] = {}

    @property
    def conectado(self) -> bool:
        return self._client is not None


    @property
    def endereco(self) -> str:
        return f"{self.host}:{self.porta}"


    @property
    def keepalive_mqtt(self) -> int:
        """Keepalive enviado ao broker: o MQTT só aceita segundos inteiros, então arredonda para cima."""
        return max(1, math.ceil(self.keepalive))


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
                    keepalive=self.keepalive_mqtt,
                ) as client:
                    self._client = client
                    logging.info(
                        f"Conectado ao broker {self.endereco} | Coletor {self.cliente_id } | "
                        f"Keepalive: {self.keepalive_mqtt}s"
                    )
                    await self._refazer_assinaturas()
                    await self._executar_tratadores(self._ao_conectar, "ao_conectar")

                    async for mensagem in client.messages:
                        await self._tratar_mensagem(mensagem)

            except aiomqtt.MqttError as e:
                sessao_perdida = self._client is not None
                # Antes dos tratadores: quem for avisado da queda precisa ver `conectado` falso
                self._client = None
                motivo = "conexão perdida" if sessao_perdida else "falha ao conectar"
                logging.error(
                    f"Erro: {BrokerIndisponivel.codigo} | Broker {self.endereco}: {motivo} ({e}) | "
                    f"nova tentativa em {self.intervalo_reconexao:g}s"
                )
                if sessao_perdida:
                    await self._executar_tratadores(self._ao_desconectar, "ao_desconectar")
            finally:
                self._client = None

            await asyncio.sleep(self.intervalo_reconexao)


    async def _refazer_assinaturas(self) -> None:
        # Cópia: um assinar() durante os awaits mudaria o dict no meio da iteração
        for topico, qos in list(self._assinaturas.items()):
            await self._client.subscribe(topico, qos=qos)
            logging.info(f"Tópico assinado: {topico} (QoS {qos})")


    async def _tratar_mensagem(self, mensagem: aiomqtt.Message) -> None:
        conteudo = Mensagem.de_json(mensagem.payload, mensagem.topic.value)
        if conteudo is None or self.ao_receber is None:
            return

        try:
            resultado = self.ao_receber(mensagem.topic.value, conteudo)
            if inspect.isawaitable(resultado):
                await resultado
        except Exception as e:
            # Um erro no tratamento de uma mensagem não pode derrubar a sessão com o broker
            logging.exception(f"Erro ao tratar mensagem do tópico {mensagem.topic.value} [{type(e).__name__}]: {e}")


    def adicionar_ao_conectar(self, tratador: TratadorConexao) -> None:
        """Registra uma função chamada a cada (re)conexão ao broker, depois de refazer as assinaturas.

        As funções rodam na ordem em que foram registradas.
        """
        self._ao_conectar.append(tratador)


    def adicionar_ao_desconectar(self, tratador: TratadorConexao) -> None:
        """Registra uma função chamada quando uma sessão ativa com o broker cai.

        Não é chamada nas tentativas de conexão que falham nem no encerramento (cancelamento).
        As funções rodam na ordem em que foram registradas.
        """
        self._ao_desconectar.append(tratador)


    async def _executar_tratadores(self, tratadores: list[TratadorConexao], evento: str) -> None:
        for tratador in tratadores:
            try:
                resultado = tratador()
                if inspect.isawaitable(resultado):
                    await resultado
            except  Exception as e:
                # Uma falha no gancho não pode derrubar a sessão com o broker nem o laço de reconexão
                logging.exception(f"Erro ao executar {evento} [{type(e).__name__}]: {e}")
