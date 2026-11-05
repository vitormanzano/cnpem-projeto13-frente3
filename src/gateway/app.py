import asyncio
import contextlib
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

from dotenv import load_dotenv

# coordenador.py importa gateway.*: precisa de src/ no caminho (no Docker já vem do PYTHONPATH)
sys.path.append(str(Path(__file__).resolve().parent.parent))

from coletor.coletor import Coletor, identificador_coletor
from coordenador.coordenador import Coordenador
from mqtt.conexao import ConexaoBroker

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

async def rodar_coletor() -> None:
    cliente_id = identificador_coletor()
    logging.info(f"Iniciando como coletor | id {cliente_id}")
    coletor = Coletor(ConexaoBroker(cliente_id=cliente_id))
    await coletor.assinar_topicos()
    await coletor.executar()


async def rodar_coordenador() -> None:
    cliente_id = os.getenv("COORDENADOR_ID", "coordenador")
    logging.info(f"Iniciando como coordenador | id {cliente_id}")
    coordenador = Coordenador(ConexaoBroker(cliente_id=cliente_id))
    await coordenador.assinar_topicos()
    await coordenador.executar()


PAPEIS: dict[str, Callable[[], Awaitable[None]]] = {
    "coletor": rodar_coletor,
    "coordenador": rodar_coordenador,
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
        raise SystemExit(f"PAPEL inválido: {papel!r}. (coletor ou coordenador)")
    asyncio.run(principal(papel))
