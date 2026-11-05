import asyncio
import os
import socket

from mqtt.domain.models import Mensagem
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

    def __init__(self, broker: ConexaoBroker) -> None:
        self.broker = broker
        self.presenca = PresencaColetor(broker) # antes de conectar: registra a última vontade
        self.broker.ao_receber = self.ao_receber


    async def assinar_topicos(self) -> None:
        """Estados e comandos publicados pelo coordenador."""
        await self.broker.assinar(Topicos.estado_posse("+"))
        await self.broker.assinar(Topicos.handoff("+"))
        await self.broker.assinar(Topicos.presenca_coordenador())


    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        """match para decidir o que fazer com cada mensagem enviada."""
        # TODO
        pass


    async def executar(self) -> None:
        """Mantém a sessão com o broker e o sinal de vida."""
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.presenca.manter_sinal_de_vida()),
        }
        try:
            await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            await self.presenca.anunciar_offline()
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)
