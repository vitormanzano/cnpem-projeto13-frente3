import asyncio
import os
import socket
import logging

from gateway.bluetooth.conexao import ConexaoPulseira
from gateway.bluetooth.domain.erros import BluetoothIndisponivel, BrokerIndisponivel, FalhaConexao
from gateway.bluetooth.reconexao import manter_conexao
from gateway.bluetooth.scanner import VarreduraPulseira
from gateway.mqtt.estado_rssi import EstadoRssiColetor
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from mqtt.presenca import PresencaColetor
from mqtt.conexao import ConexaoBroker


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

    def __init__(self, broker: ConexaoBroker, endereco_pulseira: str) -> None:
        self.broker                        = broker
        self.endereco                      = endereco_pulseira
        self.presenca                      = PresencaColetor(broker)    # antes de conectar: registra a última vontade
        self.estado_rssi                   = EstadoRssiColetor(broker)  # O online sai antes do RSSI
        self.varredura                     = VarreduraPulseira(endereco_pulseira)
        self.varredura.ao_ler              = self.estado_rssi.registrar # Cada leitura entra na fila de publicação
        self.conexao                       = ConexaoPulseira(endereco_pulseira)
        self.conexao.ao_ler                = self.estado_rssi.registrar # RSSI "conectado" para o coordenador
        self._varredura: asyncio.Task | None = None
        self._sessao: asyncio.Task | None  = None                       # Conexão enquanto tiver a psse
        self.broker.ao_receber             = self.ao_receber


    async def assinar_topicos(self) -> None:
        """Estados e comandos publicados pelo coordenador."""
        await self.broker.assinar(Topicos.estado_posse("+"))
        await self.broker.assinar(Topicos.handoff("+"))
        await self.broker.assinar(Topicos.presenca_coordenador())


    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        match mensagem.tipo:
            case TipoMensagem.POSSE:
                if Topicos.extrair_estado_posse(topico) == self.endereco:
                    await self._tratar_posse(mensagem.conteudo.get("coletor"))
            case TipoMensagem.HANDOFF:
                if (mensagem.conteudo.get("pulseira") == self.endereco
                        and mensagem.conteudo.get("origem") == self.broker.cliente_id):
                    await self._liberar("handoff")


    async def _tratar_posse(self, dono: str | None) -> None:
        if dono == self.broker.cliente_id:
            if self._sessao is None or self._sessao.done():
                logging.info(f"Posse recebida | Pulseira: {self.endereco}")
                await self._parar_varredura()
                self._sessao = asyncio.create_task(self._manter_sessao())
        elif self._sessao is not None:
            logging.warning(f"Posse perdida sem handoff | Pulseira: {self.endereco} | Dono: {dono}")
            await self._desconectar()
            self._iniciar_varredura()
                    

    async def _manter_sessao(self) -> None:
        """Roda em tarefa própria: conectar pode levar até CONNECT_TIMEOUT e não pode travar o MQTT."""
        try:
            await manter_conexao(self.conexao)
        except(FalhaConexao, BluetoothIndisponivel) as e:
            logging.error(f"Pulseira: {self.endereco} | Conexão perdida, devolvendo a posse: {e}")
            self._sessao = None
            await self._publicar_liberacao()
            self._iniciar_varredura()


    async def _liberar(self, motivo: str) -> None:
        """Desconecta, e só depois publica a liberação: o coordenador não pode conceder a posse
        a outro coletor enqunato este ainda estiver conectado.
        """
        logging.info(f"Liberando a pulseira {self.endereco} | Motivo: {motivo}")
        await self._desconectar()
        await self._publicar_liberacao()
        self._iniciar_varredura() # Volta a disputar com preferência


    async def _desconectar(self) -> None:
        if self._sessao is None:
            return
        try:
            await self.conexao.desconectar()
        except FalhaConexao as e:
            logging.error(f"Pulseira: {self.endereco} | {e}")
        self._sessao.cancel()
        await asyncio.gather(self._sessao, return_exceptions=True)
        self._sessao = None


    async def _publicar_liberacao(self) -> None:
        mensagem = Mensagem(
            tipo=TipoMensagem.LIBERACAO_POSSE,
            origem=self.broker.cliente_id,
            conteudo={
                "pulseira": self.endereco,
                }
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
        self._iniciar_varredura()
        """Mantém a sessão com o broker e o sinal de vida."""
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.presenca.manter_sinal_de_vida()),
            asyncio.create_task(self.estado_rssi.manter_publicacao()),
        }
        try:
            await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            if self._sessao is not None:
                await self._liberar("encerramento")
            await self.presenca.anunciar_offline()
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)
