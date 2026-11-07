"""Reconexão automática à pulseira (RF05)."""
import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING

from bluetooth.domain.erros import BluetoothIndisponivel, FalhaConexao

if TYPE_CHECKING:
    from bluetooth.conexao import ConexaoPulseira

TENTATIVAS_MAXIMAS_PADRAO = 5
INTERVALO_RECONEXAO_PADRAO = 3.0


def ler_configuracao_reconexao(
    max_tentativas: int | None = None, intervalo: float | None = None
) -> tuple[int, float]:
    """Resolve o limite de tentativas e o intervalo entre elas.

    Valores omitidos são lidos das variáveis de ambiente RECONEXAO_MAX_TENTATIVAS
    (padrão de 5) e RECONEXAO_INTERVALO (padrão de 3 s).

    Raises:
        ValueError: Se o limite for menor que 1, o intervalo for negativo ou a
            variável de ambiente não for um número.
    """
    if max_tentativas is None:
        max_tentativas = int(os.getenv("RECONEXAO_MAX_TENTATIVAS", TENTATIVAS_MAXIMAS_PADRAO))
    if intervalo is None:
        intervalo = float(os.getenv("RECONEXAO_INTERVALO", INTERVALO_RECONEXAO_PADRAO))

    if max_tentativas < 1:
        raise ValueError("RECONEXAO_MAX_TENTATIVAS deve ser pelo menos 1")
    if intervalo < 0:
        raise ValueError("RECONEXAO_INTERVALO não pode ser negativo")
    return max_tentativas, intervalo


async def manter_conexao(
    conexao: "ConexaoPulseira",
    max_tentativas: int | None = None,
    intervalo: float | None = None,
) -> None:
    """Conecta à pulseira e a mantém conectada, reconectando após quedas inesperadas (RF05).

    A primeira conexão não é repetida: se falhar, o erro sobe ao chamador (RF03). Depois de
    conectada, a cada queda o laço tenta reconectar até `max_tentativas` vezes, esperando
    `intervalo` segundos entre elas. O contador recomeça a cada reconexão bem-sucedida, e os
    fluxos de dados são reiniciados a cada nova conexão.

    Uma desconexão solicitada pelo coletor (RF10, handoff) encerra o laço sem reconectar.

    Args:
        conexao: Conexão com a pulseira.
        max_tentativas: Máximo de tentativas por queda. Se omitido, é lido de
            RECONEXAO_MAX_TENTATIVAS (padrão de 5).
        intervalo: Segundos entre as tentativas. Se omitido, é lido de
            RECONEXAO_INTERVALO (padrão de 3 s).

    Raises:
        FalhaConexao: Se a primeira conexão falhar ou as tentativas de reconexão se esgotarem.
        BluetoothIndisponivel: Se o Bluetooth estiver indisponível na primeira conexão.
        ValueError: Se a configuração for inválida.
    """
    max_tentativas, intervalo = ler_configuracao_reconexao(max_tentativas, intervalo)

    await conexao.conectar()
    while True:
        conectada_desde = time.monotonic()
        tarefa_rssi = await _reiniciar_fluxos(conexao)
        try:
            await conexao.aguardar_desconexao()
        finally:
            await _encerrar_tarefa(tarefa_rssi)

        if conexao.desconexao_intencional:
            logging.info(
                f"Estado: {conexao.estado.value} | Pulseira {conexao.endereco} | "
                "Desconexão solicitada pelo coletor, sem reconexão automática"
            )
            return

        queda = time.monotonic()
        logging.warning(
            f"Estado: {conexao.estado.value} | Pulseira {conexao.endereco} | "
            f"Queda inesperada após {queda - conectada_desde:.1f}s conectada; iniciando reconexão"
        )

        tentativa = await _tentar_reconectar(conexao, max_tentativas, intervalo)
        if tentativa == 0:
            motivo = f"reconexão esgotada após {max_tentativas} tentativa(s)"
            logging.error(
                f"Estado: {conexao.estado.value} | Erro: {FalhaConexao.codigo} | "
                f"Pulseira {conexao.endereco}: {motivo}"
            )
            raise FalhaConexao(motivo)

        logging.info(
            f"Estado: {conexao.estado.value} | Pulseira {conexao.endereco} | "
            f"Reconectada em {time.monotonic() - queda:.1f}s (tentativa {tentativa})"
        )


async def _tentar_reconectar(conexao: "ConexaoPulseira", max_tentativas: int, intervalo: float) -> int:
    """Tenta reconectar até o limite. Devolve o número da tentativa que deu certo, ou 0 se esgotou."""
    for tentativa in range(1, max_tentativas + 1):
        logging.info(
            f"Estado: {conexao.estado.value} | Pulseira {conexao.endereco} | "
            f"Reconexão: tentativa {tentativa}/{max_tentativas}"
        )
        try:
            await conexao.conectar()
            return tentativa
        except (FalhaConexao, BluetoothIndisponivel) as e:
            logging.warning(
                f"Pulseira {conexao.endereco} | Reconexão: tentativa {tentativa}/{max_tentativas} "
                f"falhou [{e.codigo}]: {e}"
            )

        if tentativa < max_tentativas:
            await asyncio.sleep(intervalo)
    return 0


async def _reiniciar_fluxos(conexao: "ConexaoPulseira") -> asyncio.Task:
    """Reinicia os fluxos de dados da pulseira, necessários a cada nova conexão.

    Reassina a frequência cardíaca e retoma o monitoramento de RSSI, devolvendo a tarefa
    do monitor. Uma falha na assinatura é registrada, mas não derruba a conexão nem o RSSI.
    Streams que a Polar exige iniciar por comando (PMD) devem ser iniciados aqui também.
    """
    try:
        await conexao.iniciar_leitura_frequencia_cardiaca()
    except Exception as e:  # noqa: BLE001 - nenhuma falha aqui deve encerrar o laço de reconexão
        logging.error(
            f"Pulseira {conexao.endereco} | Não foi possível assinar a frequência cardíaca "
            f"[{type(e).__name__}]: {e}"
        )
    return asyncio.create_task(conexao.monitorar_rssi())


async def _encerrar_tarefa(tarefa: asyncio.Task) -> None:
    tarefa.cancel()
    await asyncio.gather(tarefa, return_exceptions=True)
