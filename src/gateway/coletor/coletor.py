import asyncio
import os
import socket
import logging

from bluetooth.conexao import ConexaoPulseira
from bluetooth.domain.classificacao_conexao import ClassificacaoConexao
from bluetooth.domain.erros import BluetoothIndisponivel, BrokerIndisponivel, FalhaConexao
from bluetooth.domain.models import EstadoConexao
from bluetooth.reconexao import ler_configuracao_reconexao, manter_conexao
from bluetooth.scanner import VarreduraPulseira
from mqtt.estado_rssi import EstadoRssiColetor
from mqtt.frequencia_cardiaca import FrequenciaCardiacaColetor
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from mqtt.presenca import PresencaColetor
from mqtt.conexao import ConexaoBroker


PRAZO_SEM_BROKER_PADRAO = 15.0


def ler_prazo_sem_broker(prazo: float | None = None) -> float:
    """Resolve o tempo sem broker, com a pulseira conectada, até soltá-la para a disputa (RF11).

    Se omitido, é lido de PRAZO_SEM_BROKER (padrão de 15 s).

    Raises:
        ValueError: Se o prazo não for um número maior que zero.
    """
    if prazo is None:
        valor = os.getenv("PRAZO_SEM_BROKER") or str(PRAZO_SEM_BROKER_PADRAO)
        try:
            prazo = float(valor)
        except ValueError as e:
            raise ValueError(f"PRAZO_SEM_BROKER deve ser um número, recebido {valor!r}.") from e

    if prazo <= 0:
        raise ValueError("PRAZO_SEM_BROKER deve ser maior que zero.")
    return prazo


def identificador_coletor() -> str:
    """Id do coletor (client id MQTT e {coletor} nos tópicos): o hostname da máquina.

    No Docker, só é o hostname do host porque o serviço usa network_mode: host; na rede padrão
    seria o id do container, que muda a cada recriação. Cada coletor precisa de um hostname único,
    senão o broker derruba uma sessão sempre que a outra conecta com o mesmo client id.
    COLETOR_ID, se definido, tem prioridade (ex.: dois coletores na mesma máquina em teste).
    """
    return os.getenv("COLETOR_ID") or socket.gethostname()


class Coletor:
    """Coletor BLE: Realiza todas as ações que um coletor precisa."""

    def __init__(
        self,
        broker: ConexaoBroker,
        endereco_pulseira: str,
        prazo_sem_broker: float | None = None,
    ) -> None:
        """
        Args:
            broker (ConexaoBroker): Sessão MQTT do coletor.
            endereco_pulseira (str): Endereço BLE da pulseira.
            prazo_sem_broker (float | None): Segundos sem broker, com a pulseira conectada, até
                desconectar dela para que possa ser disputada (RF11). Se omitido, é lido de
                PRAZO_SEM_BROKER (padrão de 15 s).

        Raises:
            ValueError: Se a configuração da conexão, da reconexão, do prazo sem broker ou do buffer
                de frequênci cardíaca for inválida
        """
        self.broker                                           = broker
        self.endereco                                         = endereco_pulseira
        self.presenca                                         = PresencaColetor(broker)    # antes de conectar: registra a última vontade
        self.estado_rssi                                      = EstadoRssiColetor(broker)  # O online sai antes do RSSI
        self.frequencia                                       = FrequenciaCardiacaColetor(broker, endereco_pulseira)
        self._classificacao                                   = ClassificacaoConexao()     # Um estado só, da varredura à conexão (RF08)
        self.varredura                                        = VarreduraPulseira(endereco_pulseira, classificacao=self._classificacao)
        self.varredura.ao_ler                                 = self.estado_rssi.registrar # Cada leitura entra na fila de publicação
        self.conexao                                          = ConexaoPulseira(endereco_pulseira, classificacao=self._classificacao)
        self.conexao.ao_atualizar                             = self.estado_rssi.registrar # Estado e RSSI da conexão, inclusive o desconectado (RF10)
        self.conexao.ao_ler_frequencia_cardiaca               = self.frequencia.registrar  # Publicada só com a posse (RF17)
        self._varredura: asyncio.Task | None                  = None
        self._sessao: asyncio.Task | None                     = None                       # Conexão enquanto tiver a psse
        self.broker.ao_receber                                = self.ao_receber
        # Lida aqui e não ao receber a posse: inválida, derrubaria só a sessão e a posse ficaria presa
        self._tentativas_reconexao, self._intervalo_reconexao = ler_configuracao_reconexao()
        self.prazo_sem_broker                                 = ler_prazo_sem_broker(prazo_sem_broker)
        self._sem_broker: asyncio.Task | None                 = None                       # Espera do prazo sem broker (RF11)
        self._soltando_sem_broker                             = False
        self.broker.adicionar_ao_desconectar(self._iniciar_prazo_sem_broker)
        self.broker.adicionar_ao_conectar(self._cancelar_prazo_sem_broker)


    @property
    def estado(self) -> EstadoConexao:
        """Estado da conexão com a pulseira (RF08): o da varredura sem a posse, o da conexão GATT com ela."""
        return self._classificacao.atual


    async def assinar_topicos(self) -> None:
        """Presença e sinal de vida do coordenador, e posse e handoff da pulseira configurada (RF13)."""
        await self.broker.assinar(Topicos.estado_posse(self.endereco))
        await self.broker.assinar(Topicos.handoff(self.endereco))
        await self.broker.assinar(Topicos.presenca_coordenador())
        await self.broker.assinar(Topicos.sinal_de_vida_coordenador(), qos=0)


    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        match mensagem.tipo:
            case TipoMensagem.POSSE:
                if Topicos.extrair_estado_posse(topico) == self.endereco:
                    await self._tratar_posse(mensagem.conteudo.get("coletor"))
            case TipoMensagem.HANDOFF:
                if (Topicos.extrair_handoff(topico) == self.endereco
                        and mensagem.conteudo.get("origem") == self.broker.cliente_id):
                    await self._liberar("handoff", handoff=mensagem.conteudo.get("id"))
                    self._iniciar_varredura() # Volta a disputar como favorito


    async def _tratar_posse(self, dono: str | None) -> None:
        """Posse minha: conecta. Liberada: começa a varrer. Com outro coletor: para de varrer.

        A pulseira atendida na presença (RF12) segue a posse publicada pelo coordenador, que é
        quem concede e libera a posse.
        """
        if dono == self.broker.cliente_id:
            self.frequencia.receber_posse() # Também a posse retida de cada reconexão: libera o buffer (RF17)
            if self._sessao is None or self._sessao.done():
                logging.info(f"Posse recebida | Pulseira: {self.endereco}")
                await self._parar_varredura()
                self._sessao = asyncio.create_task(self._manter_sessao())
            await self.presenca.atualizar_pulseira(self.endereco)
            return
        # Antes de desconectar: leituras que ainda cheguem sairiam duplicadas com as do novo dono (RF17)
        self.frequencia.perder_posse()
        if self._sessao is not None:
            logging.warning(f"Posse perdida sem handoff | Pulseira: {self.endereco} | Dono: {dono}")
            await self._desconectar()
        await self.presenca.atualizar_pulseira(None)

        if dono is None:
            self._iniciar_varredura() # Posse liberada
        else:
            await self._parar_varredura() # Conectada a outro coletor

    async def _manter_sessao(self) -> None:
        """Roda em tarefa própria: conectar pode levar até CONNECT_TIMEOUT e não pode travar o MQTT."""
        try:
            await manter_conexao(self.conexao, self._tentativas_reconexao, self._intervalo_reconexao)
        except(FalhaConexao, BluetoothIndisponivel) as e:
            logging.error(f"Pulseira: {self.endereco} | Conexão perdida, devolvendo a posse: {e}")
            self._sessao = None
            await self.estado_rssi.publicar_pendentes()
            await self.frequencia.publicar_pendentes()
            self.frequencia.perder_posse()
            await self._publicar_liberacao()
            self._iniciar_varredura()


    async def _liberar(self, motivo: str, handoff: str | None = None) -> None:
        """Desconecta, publica o estado desconectado e só depois a liberação: o coordenador
        não pode conceder a posse a outro coletor enquanto este ainda estiver conectado.

        Args:
            motivo (str): Motivo registrado no log.
            handoff (str | None): Id do comando de handoff que pediu a liberação (RF06).
        """
        logging.info(
            f"Liberando a pulseira {self.endereco} | Motivo: {motivo}"
            + (f" | Handoff: {handoff}" if handoff else "")
        )
        await self._desconectar()
        await self.estado_rssi.publicar_pendentes()
        # As leituras deste coletor saem antes da liberação, e portanto antes das do novo dono (RF06)
        await self.frequencia.publicar_pendentes()
        self.frequencia.perder_posse()
        await self._publicar_liberacao(handoff)


    async def _desconectar(self) -> None:
        # Guarda a sessão: o prazo sem broker e uma posse alheia podem desconectar ao mesmo tempo
        sessao = self._sessao
        if sessao is None:
            return
        try:
            await self.conexao.desconectar()
        except FalhaConexao as e:
            logging.error(f"Pulseira: {self.endereco} | {e}")
        sessao.cancel()
        await asyncio.gather(sessao, return_exceptions=True)
        if self._sessao is sessao:
            self._sessao = None


    def _iniciar_prazo_sem_broker(self) -> None:
        """Chamado quando a sessão com o broker cai. Sem a pulseira, não há o que soltar."""
        if self._sessao is None:
            return
        if self._sem_broker is None or self._sem_broker.done():
            logging.warning(
                f"Sem broker | Pulseira: {self.endereco} | Desconecta em {self.prazo_sem_broker:g}s "
                f"se o broker não voltar"
            )
            self._sem_broker = asyncio.create_task(self._soltar_apos_prazo())


    async def _cancelar_prazo_sem_broker(self) -> None:
        """Chamado a cada (re)conexão ao broker: o broker voltou antes do prazo."""
        # Só cancela a espera: a desconexão já começada vai até o fim, senão a pulseira ficaria meio solta
        if self._sem_broker is None or self._soltando_sem_broker:
            return
        self._sem_broker.cancel()
        await asyncio.gather(self._sem_broker, return_exceptions=True)
        self._sem_broker = None


    async def _soltar_apos_prazo(self) -> None:
        """Sem broker o coordenador não enxerga este coletor e libera a posse dele (via última
        vontade); a pulseira precisa estar solta para outro coletor vencer a disputa (RF11).

        A desconexão é a intencional (RF10), sem reconexão automática. A liberação não é publicada,
        porque não há broker; na reconexão a presença sai sem pulseira e a posse retida decide o resto,
        inclusive se as leituras de frequência cardíaca guardadas são publicadas ou descartadas (RF17).
        """
        await asyncio.sleep(self.prazo_sem_broker)
        if self.broker.conectado or self._sessao is None:
            return

        self._soltando_sem_broker = True
        try:
            logging.warning(
                f"Liberando a pulseira {self.endereco} | Motivo: sem broker há {self.prazo_sem_broker:g}s"
            )
            await self._desconectar()
            # Falha agora (sem broker), mas guarda a pulseira atendida para o online da reconexão
            await self.presenca.atualizar_pulseira(None)
        finally:
            self._soltando_sem_broker = False


    async def _publicar_liberacao(self, handoff: str | None = None) -> None:
        """Publica a liberação da posse; com `handoff`, ela responde a esse comando (RF06, RF15)."""
        conteudo = {"pulseira": self.endereco}
        if handoff is not None:
            conteudo["handoff"] = handoff
        mensagem = Mensagem(
            tipo=TipoMensagem.LIBERACAO_POSSE,
            origem=self.broker.cliente_id,
            conteudo=conteudo,
        )
        try:
            await self.broker.publicar(Topicos.liberacao_posse(self.endereco), mensagem)
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {self.endereco} | Liberação não publicada: {e}")


    def _iniciar_varredura(self) -> None:
        if self._varredura is None or self._varredura.done():
            self._varredura = asyncio.create_task(self.varredura.executar())


    async def _parar_varredura(self) -> None:
        if self._varredura is not None:
            self._varredura.cancel()
            await asyncio.gather(self._varredura, return_exceptions=True)
            self._varredura = None


    async def executar(self) -> None:
        """Mantém a sessão com o broker e o sinal de vida."""
        self._iniciar_varredura() # Sem posse retida
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.presenca.manter_sinal_de_vida()),
            asyncio.create_task(self.estado_rssi.manter_publicacao()),
            asyncio.create_task(self.frequencia.manter_publicacao()),
        }
        try:
            await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            await self._parar_varredura()
            if self._sem_broker is not None:
                self._sem_broker.cancel()
                await asyncio.gather(self._sem_broker, return_exceptions=True)
                self._sem_broker = None
            if self._sessao is not None:
                await self._liberar("encerramento")
            await self.presenca.anunciar_offline()
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)
