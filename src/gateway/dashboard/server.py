"""Dashboard local da pulseira: frequência cardíaca + força do sinal.

Uso (na raiz do projeto):
    python src/gateway/dashboard/server.py              # pulseira real
    python src/gateway/dashboard/server.py --simulate   # dados falsos, sem pulseira

Depois abra http://127.0.0.1:8765 (abre sozinho, a não ser com --no-browser).

Sem dependências além do Bleak: o servidor HTTP/SSE usa só a biblioteca padrão.
Por privacidade, escuta apenas em 127.0.0.1; use --host 0.0.0.0 para abrir a
página em outro computador da rede.
"""
import argparse
import asyncio
import json
import os
import sys
import webbrowser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hub import Hub  # noqa: E402

STATIC_DIR = Path(__file__).resolve().parent / "static"


# --------------------------------------------------------------------------
# Servidor HTTP mínimo: página, histórico (JSON) e fluxo ao vivo (SSE)
# --------------------------------------------------------------------------
def _resposta(status: str, content_type: str, body: bytes) -> bytes:
    cabecalho = (
        f"HTTP/1.1 {status}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Cache-Control: no-store\r\n"
        "Connection: close\r\n\r\n"
    )
    return cabecalho.encode("latin-1") + body


async def _tratar_cliente(hub: Hub, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        linha = await asyncio.wait_for(reader.readline(), timeout=10)
        while True:  # descarta os cabeçalhos
            h = await asyncio.wait_for(reader.readline(), timeout=10)
            if h in (b"\r\n", b"\n", b""):
                break

        partes = linha.decode("latin-1").split()
        if len(partes) < 2 or partes[0] != "GET":
            writer.write(_resposta("405 Method Not Allowed", "text/plain; charset=utf-8", b"Metodo nao permitido"))
            return
        caminho = partes[1].split("?")[0]

        if caminho == "/":
            html = (STATIC_DIR / "index.html").read_bytes()
            writer.write(_resposta("200 OK", "text/html; charset=utf-8", html))
        elif caminho == "/api/history":
            corpo = json.dumps(list(hub.history)).encode()
            writer.write(_resposta("200 OK", "application/json", corpo))
        elif caminho == "/api/services":
            corpo = json.dumps({"device": hub.device, "services": hub.services}).encode()
            writer.write(_resposta("200 OK", "application/json", corpo))
        elif caminho == "/events":
            await _fluxo_sse(hub, writer)
        else:
            writer.write(_resposta("404 Not Found", "text/plain; charset=utf-8", b"Nao encontrado"))
        await writer.drain()
    except (ConnectionError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        pass  # navegador fechou a aba / conexão ociosa
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def _fluxo_sse(hub: Hub, writer: asyncio.StreamWriter) -> None:
    writer.write(
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/event-stream\r\n"
        b"Cache-Control: no-store\r\n"
        b"Connection: keep-alive\r\n\r\n"
    )
    await writer.drain()
    fila = hub.subscribe()
    try:
        while True:
            try:
                snap = await asyncio.wait_for(fila.get(), timeout=15)
                writer.write(f"data: {json.dumps(snap)}\n\n".encode())
            except asyncio.TimeoutError:
                writer.write(b": ping\n\n")
            await writer.drain()
    finally:
        hub.unsubscribe(fila)


# --------------------------------------------------------------------------
async def principal(args: argparse.Namespace) -> None:
    hub = Hub()
    tarefas = [asyncio.create_task(hub.run_ticker())]

    if args.simulate:
        import simulator
        tarefas.append(asyncio.create_task(simulator.run(hub)))
    else:
        from sensor import PolarSensor
        sensor = PolarSensor(hub, address=args.address, name_hint=args.name)
        tarefas.append(asyncio.create_task(sensor.run()))

    servidor = await asyncio.start_server(
        lambda r, w: _tratar_cliente(hub, r, w), args.host, args.port
    )
    url = f"http://{'127.0.0.1' if args.host == '0.0.0.0' else args.host}:{args.port}"
    modo = "SIMULADO" if args.simulate else "pulseira real"
    print(f"Dashboard no ar ({modo}): {url}   (Ctrl+C para sair)")
    if not args.no_browser:
        webbrowser.open(url)

    try:
        async with servidor:
            await servidor.serve_forever()
    finally:
        for t in tarefas:
            t.cancel()


def ler_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Dashboard de FC e sinal da Polar Verity Sense")
    p.add_argument("--simulate", action="store_true", help="usa dados simulados (sem pulseira)")
    p.add_argument("--address", default=os.getenv("TARGET_ADDRESS"),
                   help="MAC (Linux/Windows) ou UUID (macOS) da pulseira; padrão: variável TARGET_ADDRESS")
    p.add_argument("--name", default=os.getenv("TARGET_NAME", "Polar"),
                   help="trecho do nome anunciado, usado quando não há endereço (padrão: Polar)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--no-browser", action="store_true", help="não abre o navegador sozinho")
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(principal(ler_argumentos()))
    except KeyboardInterrupt:
        print("\nEncerrado.")
