"""Testes do ponto de entrada (app.py): escolha do papel e encerramento, sem hardware nem broker.

Rodar na raiz do projeto:  python tests/test_app.py
(também funciona com pytest)
"""
import asyncio
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path

GATEWAY = Path(__file__).resolve().parent.parent / "src" / "gateway"
sys.path.insert(0, str(GATEWAY))

import app  # noqa: E402


def test_papeis_disponiveis() -> None:
    assert set(app.PAPEIS) == {"coletor", "coordenador", "painel"}


def test_principal_roda_o_papel_escolhido_e_registra_o_encerramento() -> None:

    if sys.platform == "win32":  # o asyncio do Windows não tem add_signal_handler: só roda em Linux e macOS
        return
    
    chamadas: list[str] = []

    async def papel_falso() -> None:
        chamadas.append("rodou")

    original = app.PAPEIS["painel"]
    app.PAPEIS["painel"] = papel_falso
    registros: list[str] = []

    class Captura(logging.Handler):
        def emit(self, registro: logging.LogRecord) -> None:
            registros.append(registro.getMessage())

    captura = Captura()
    logging.getLogger().addHandler(captura)
    try:
        asyncio.run(app.principal("painel"))
    finally:
        app.PAPEIS["painel"] = original
        logging.getLogger().removeHandler(captura)

    assert chamadas == ["rodou"]
    assert "Painel encerrado." in registros


def test_principal_trata_o_cancelamento_como_encerramento_normal() -> None:
    """CTRL-C e 'docker stop' cancelam a tarefa: o papel deve terminar sem propagar CancelledError."""

    if sys.platform == "win32":  # o asyncio do Windows não tem add_signal_handler: só roda em Linux e macOS
        return

    async def papel_demorado() -> None:
        await asyncio.sleep(60)

    original = app.PAPEIS["painel"]
    app.PAPEIS["painel"] = papel_demorado

    async def cenario() -> None:
        tarefa = asyncio.create_task(app.principal("painel"))
        await asyncio.sleep(0.05)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(tarefa, timeout=5)

    try:
        asyncio.run(cenario())
    finally:
        app.PAPEIS["painel"] = original


def rodar_app(papel: str | None) -> subprocess.CompletedProcess[str]:
    ambiente = {k: v for k, v in os.environ.items() if k != "PAPEL"}
    if papel is not None:
        ambiente["PAPEL"] = papel
    return subprocess.run(
        [sys.executable, str(GATEWAY / "app.py")],
        capture_output=True, text=True, timeout=30, env=ambiente, cwd=GATEWAY,
    )


def test_papel_invalido_encerra_com_mensagem() -> None:
    resultado = rodar_app("goleiro")
    assert resultado.returncode != 0
    assert "PAPEL inválido" in resultado.stderr
    assert "coletor, coordenador ou painel" in resultado.stderr


def test_papel_ausente_encerra_com_mensagem() -> None:
    resultado = rodar_app(None)
    assert resultado.returncode != 0
    assert "PAPEL inválido" in resultado.stderr


if __name__ == "__main__":
    testes = [(nome, f) for nome, f in sorted(globals().items()) if nome.startswith("test_") and callable(f)]
    for nome, funcao in testes:
        funcao()
        print(f"ok  {nome}")
    print(f"\n{len(testes)} testes passaram.")
