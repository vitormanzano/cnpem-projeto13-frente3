import asyncio

from mqtt.domain.models import Mensagem
from mqtt.domain.topicos import Topicos
from mqtt.presenca import PresencaColetor
from mqtt.conexao import ConexaoBroker


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


    async def exeuctar(self) -> None:
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
