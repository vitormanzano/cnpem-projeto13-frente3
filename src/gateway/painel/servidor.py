import asyncio
import json
import logging
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from painel.painel import Painel


HOST_PADRAO       = "0.0.0.0" # Rede local: o operador abre o painel de qualquer computador do laboratório (RF20)
PORTA_PADRAO      = 8765
TEMPO_PEDIDO      = 10.0      # s para o navegador enviar o pedido HTTP
MAXIMO_CABECALHOS = 100
REPETIR_APOS_MS   = 2000      # Espera do EventSource antes de reconectar, se o servidor cair

# Lida na importação, e não a cada pedido, para não haver I/O síncrono dentro das corrotinas.
# A página não carrega nada de fora (CSS e JS embutidos), para funcionar sem internet (RF20, RNF03).
PAGINA = (Path(__file__).resolve().parent / "static" / "index.html").read_bytes()


def ler_porta_painel(porta: int | None = None) -> int:
    """Resolve a porta HTTP do painel (RF20).

    Args:
        porta (int | None): Porta. Se omitida, é lida de PAINEL_PORTA (padrão: 8765). 0 deixa o
            sistema escolher uma porta livre (usado nos testes).

    Raises:
        ValueError: Se a porta não for um inteiro entre 0 e 65535.
    """
    if porta is None:
        valor = os.getenv("PAINEL_PORTA") or str(PORTA_PADRAO)
        try:
            porta = int(valor)
        except ValueError as e:
            raise ValueError(f"PAINEL_PORTA deve ser um número inteiro, recebido {valor!r}.") from e

    if not 0 <= porta <= 65535:
        raise ValueError(f"PAINEL_PORTA deve estar entre 1 e 65535 (0 escolhe uma livre), recebido {porta}.")
    return porta


class ServidorPainel:
    """Servidor HTTP mínimo do painel: a página e o fluxo de atualizações (Server-Sent Events).

    Rotas:
        GET  /                      a página
        GET  /eventos               evento "inicio" com o histórico, depois uma atualização por segundo
        POST /api/zerar?pulseira=X  zera mínimo, média e máximo da frequência cardíaca da pulseira
    """

    def __init__(self, painel: Painel, host: str | None = None, porta: int | None = None) -> None:
        """
        Args:
            painel (Painel): De onde vêm os eventos.
            host (str | None): Endereço de escuta. Se omitido, é lido de PAINEL_HOST (padrão: 0.0.0.0).
            porta (int | None): Porta. Se omitida, é lida de PAINEL_PORTA (padrão: 8765).

        Raises:
            ValueError: Se a porta for inválida.
        """
        self.painel = painel
        self.host = host or os.getenv("PAINEL_HOST") or HOST_PADRAO
        self.porta = ler_porta_painel(porta)
        self._servidor: asyncio.Server | None = None
        self._clientes: set[asyncio.Task] = set()


    @property
    def porta_em_uso(self) -> int | None:
        """Porta efetiva depois de iniciar (difere de `porta` quando ela é 0)."""
        if self._servidor is None or not self._servidor.sockets:
            return None
        return self._servidor.sockets[0].getsockname()[1]


    async def executar(self) -> None:
        """Atende os navegadores até a tarefa ser cancelada.

        Raises:
            OSError: Se não for possível escutar na porta (ex.: já está em uso).
        """
        self._servidor = await asyncio.start_server(self._tratar_cliente, self.host, self.porta)
        logging.info(
            f"Painel no ar | http://{self.host}:{self.porta_em_uso} | "
            f"Pulseiras: {', '.join(self.painel.visao.pulseiras)}"
        )
        try:
            # O start_server já aceita conexões: esta tarefa só espera o cancelamento. Sem serve_forever,
            # porque no Python 3.12+ ele espera as conexões abertas fecharem, e o fluxo de eventos não fecha.
            await asyncio.get_running_loop().create_future()
        finally:
            self._servidor.close()
            for tarefa in list(self._clientes):
                tarefa.cancel()
            await asyncio.gather(*self._clientes, return_exceptions=True)
            await self._servidor.wait_closed()
            self._servidor = None


    async def _tratar_cliente(self, leitor: asyncio.StreamReader, escritor: asyncio.StreamWriter) -> None:
        # Guardada para o encerramento: cada conexão roda numa tarefa criada pelo start_server
        tarefa = asyncio.current_task()
        self._clientes.add(tarefa)
        try:
            metodo, caminho, parametros = await self._ler_pedido(leitor)
            await self._responder(metodo, caminho, parametros, escritor)
        except (ConnectionError, asyncio.IncompleteReadError, TimeoutError, ValueError) as e:
            # Navegador fechou a aba, ficou mudo ou mandou um pedido inválido: só esta conexão termina
            logging.debug(f"Painel | Conexão HTTP encerrada [{type(e).__name__}]: {e}")
        finally:
            self._clientes.discard(tarefa)
            escritor.close()


    async def _ler_pedido(self, leitor: asyncio.StreamReader) -> tuple[str, str, dict[str, list[str]]]:
        linha = await asyncio.wait_for(leitor.readline(), TEMPO_PEDIDO)
        for _ in range(MAXIMO_CABECALHOS):
            # Os cabeçalhos não são usados; só precisam ser consumidos
            cabecalho = await asyncio.wait_for(leitor.readline(), TEMPO_PEDIDO)
            if cabecalho in (b"\r\n", b"\n", b""):
                break
        else:
            raise ValueError("cabeçalhos demais")

        partes = linha.decode("latin-1").split()
        if len(partes) < 2:
            raise ValueError(f"pedido HTTP inválido: {linha[:80]!r}")
        url = urlsplit(partes[1])
        return partes[0].upper(), url.path, parse_qs(url.query)


    async def _responder(
        self,
        metodo: str,
        caminho: str,
        parametros: dict[str, list[str]],
        escritor: asyncio.StreamWriter,
    ) -> None:
        match (metodo, caminho):
            case ("GET", "/"):
                escritor.write(_resposta("200 OK", "text/html; charset=utf-8", PAGINA))
            case ("GET", "/eventos"):
                await self._enviar_eventos(escritor)
                return
            case ("POST", "/api/zerar"):
                pulseira = parametros.get("pulseira", [""])[0].strip().upper()
                if self.painel.zerar_estatisticas(pulseira):
                    escritor.write(_resposta("204 No Content", "text/plain; charset=utf-8", b""))
                else:
                    escritor.write(_resposta("404 Not Found", "text/plain; charset=utf-8", "Pulseira não acompanhada".encode()))
            case (_, "/" | "/eventos" | "/api/zerar"):
                escritor.write(_resposta("405 Method Not Allowed", "text/plain; charset=utf-8", "Método não permitido".encode()))
            case _:
                escritor.write(_resposta("404 Not Found", "text/plain; charset=utf-8", "Não encontrado".encode()))
        await escritor.drain()


    async def _enviar_eventos(self, escritor: asyncio.StreamWriter) -> None:
        """Fluxo SSE: o "inicio" traz o histórico; depois, uma atualização por segundo com os registros novos.

        Se o navegador reconectar (queda do servidor ou da rede), recebe outro "inicio" e se ressincroniza.
        """
        escritor.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream; charset=utf-8\r\n"
            b"Cache-Control: no-store\r\n"
            b"Connection: keep-alive\r\n\r\n"
            + f"retry: {REPETIR_APOS_MS}\n\n".encode()
        )
        # O seq é lido junto do histórico, sem await no meio: o que chegar depois vai na próxima atualização
        inicial, seq = self.painel.evento_inicial()
        escritor.write(_evento(inicial, "inicio"))
        await escritor.drain()
        logging.debug("Painel | Navegador conectado ao fluxo de eventos")

        while True:
            await self.painel.aguardar_atualizacao()
            atualizacao, seq = self.painel.evento_atualizacao(seq)
            escritor.write(_evento(atualizacao))
            # Levanta ConnectionError quando o navegador fecha a aba, o que encerra o fluxo
            await escritor.drain()


def _resposta(status: str, tipo: str, corpo: bytes) -> bytes:
    cabecalho = (
        f"HTTP/1.1 {status}\r\n"
        f"Content-Type: {tipo}\r\n"
        f"Content-Length: {len(corpo)}\r\n"
        "Cache-Control: no-store\r\n"
        "Connection: close\r\n\r\n"
    )
    return cabecalho.encode("latin-1") + corpo


def _evento(dados: dict, nome: str | None = None) -> bytes:
    # json.dumps não gera quebras de linha, que encerrariam o campo data do SSE
    prefixo = f"event: {nome}\n" if nome else ""
    return f"{prefixo}data: {json.dumps(dados, ensure_ascii=False)}\n\n".encode("utf-8")
