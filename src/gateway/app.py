import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Awaitable, Callable

from dotenv import load_dotenv

from coletor.coletor import Coletor, identificador_coletor
from coordenador.coordenador import Coordenador
from bluetooth.scanner import ler_endereco_pulseira
from mqtt.conexao import ConexaoBroker
from painel.painel import Painel, identificador_painel, ler_modo_simulado, ler_pulseiras_painel
from painel.servidor import ServidorPainel
from painel.simulador import PULSEIRA_SIMULADA, SimuladorPulseira

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)

# Bibliotecas que ficam muito verbosas em DEBUG
logging.getLogger("bleak").setLevel(logging.WARNING)
logging.getLogger("aiomqtt").setLevel(logging.WARNING)

load_dotenv()

async def rodar_coletor() -> None:
    cliente_id = identificador_coletor()
    try:
        endereco = ler_endereco_pulseira()
    except ValueError as e:
        raise SystemExit(str(e)) from e
    logging.info(f"Iniciando como coletor | id {cliente_id}")
    coletor = Coletor(ConexaoBroker(cliente_id=cliente_id), endereco)
    await coletor.assinar_topicos()
    await coletor.executar()


async def rodar_coordenador() -> None:
    cliente_id = os.getenv("COORDENADOR_ID", "coordenador")
    logging.info(f"Iniciando como coordenador | id {cliente_id}")
    coordenador = Coordenador(ConexaoBroker(cliente_id=cliente_id))
    await coordenador.assinar_topicos()
    await coordenador.executar()


async def rodar_painel() -> None:
    """Painel (RF20): consome o broker e serve a página. Com PAINEL_SIMULADO, sobe também o simulador."""
    try:
        pulseiras = ler_pulseiras_painel()
        simulado = ler_modo_simulado()
        if simulado and PULSEIRA_SIMULADA not in pulseiras:
            pulseiras.append(PULSEIRA_SIMULADA)
        cliente_id = identificador_painel()
        painel = Painel(ConexaoBroker(cliente_id=cliente_id), pulseiras, simulado=simulado)
        servidor = ServidorPainel(painel)
        servicos = [servidor.executar]
        if simulado:
            simulador = SimuladorPulseira(
                coordenador=ConexaoBroker(cliente_id=f"{cliente_id}-coordenador-simulado"),
                coletores=[ConexaoBroker(cliente_id=f"{cliente_id}-coletor-simulado-{letra}") for letra in "ab"],
            )
            servicos.append(simulador.executar)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    logging.info(f"Iniciando como painel | id {cliente_id}" + (" | modo simulado" if simulado else ""))
    await painel.assinar_topicos()
    await painel.executar(*servicos)


PAPEIS: dict[str, Callable[[], Awaitable[None]]] = {
    "coletor": rodar_coletor,
    "coordenador": rodar_coordenador,
    "painel": rodar_painel,
}


async def principal(papel: str) -> None:
    """Roda o papel até CTRL-C ou 'docker stop'. Nos dois casos o executar() anuncia offline antes de sair."""
    tarefa = asyncio.current_task() 
    laco = asyncio.get_running_loop()
    for sinal in (signal.SIGINT, signal.SIGTERM):
        laco.add_signal_handler(sinal, tarefa.cancel)

    with contextlib.suppress(asyncio.CancelledError):
        await PAPEIS[papel]()
    logging.info(f"{papel.capitalize()} encerrado.")


if __name__ == "__main__":
    papel = os.getenv("PAPEL", "").strip().lower()
    if papel not in PAPEIS:
        raise SystemExit(f"PAPEL inválido: {papel!r}. (coletor, coordenador ou painel)")
    asyncio.run(principal(papel))
