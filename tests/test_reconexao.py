"""Testes da reconexão automática (RF05), sem hardware e sem Bleak.

Rodar na raiz do projeto:  python tests/test_reconexao.py
(também funciona com pytest)
"""
import asyncio
import os
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.domain.erros import BluetoothIndisponivel, FalhaConexao  # noqa: E402
from bluetooth.domain.models import EstadoConexao  # noqa: E402
from bluetooth.reconexao import (  # noqa: E402
    INTERVALO_RECONEXAO_PADRAO,
    TENTATIVAS_MAXIMAS_PADRAO,
    ler_configuracao_reconexao,
    manter_conexao,
)


class ConexaoFalsa:
    """Dublê de ConexaoPulseira: reproduz só o contrato que o RF05 usa.

    `roteiro` define o resultado de cada chamada a conectar(): "ok", "falha" ou "bluetooth".
    """

    def __init__(self, roteiro: list[str]) -> None:
        self.endereco = "AA:BB:CC:DD:EE:FF"
        self.estado = EstadoConexao.DESCONECTADO
        self.falhar_assinatura_fc = False
        self.chamadas_conectar = 0
        self.conexoes_ok = 0
        self.assinaturas_fc = 0
        self.monitores_rssi = 0
        self.monitores_rssi_cancelados = 0
        self._roteiro = list(roteiro)
        self._intencional = False
        self._desconectado = asyncio.Event()

    @property
    def desconexao_intencional(self) -> bool:
        return self._intencional

    async def conectar(self) -> None:
        self.chamadas_conectar += 1
        resultado = self._roteiro.pop(0)
        if resultado == "falha":
            raise FalhaConexao("dispositivo ausente")
        if resultado == "bluetooth":
            raise BluetoothIndisponivel("adaptador desligado")
        self.conexoes_ok += 1
        self._intencional = False
        self._desconectado.clear()
        self.estado = EstadoConexao.CONECTADO

    async def iniciar_leitura_frequencia_cardiaca(self) -> None:
        self.assinaturas_fc += 1
        if self.falhar_assinatura_fc:
            raise RuntimeError("característica inválida")

    async def monitorar_rssi(self) -> None:
        self.monitores_rssi += 1
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.monitores_rssi_cancelados += 1
            raise

    async def aguardar_desconexao(self) -> None:
        await self._desconectado.wait()

    def derrubar(self, intencional: bool = False) -> None:
        """Simula o fim da conexão: queda inesperada ou desconexão pedida pelo coletor."""
        self._intencional = intencional
        self.estado = EstadoConexao.DESCONECTADO
        self._desconectado.set()


async def aguardar(condicao, tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


async def aguardar_erro(tarefa: asyncio.Task, tipo: type[Exception]) -> None:
    try:
        await asyncio.wait_for(tarefa, 2.0)
    except tipo:
        return
    raise AssertionError(f"deveria ter lançado {tipo.__name__}")


@contextmanager
def ambiente(**variaveis: str | None):
    """Define variáveis de ambiente por um trecho (None remove) e restaura depois."""
    antes = {nome: os.environ.get(nome) for nome in variaveis}
    try:
        for nome, valor in variaveis.items():
            if valor is None:
                os.environ.pop(nome, None)
            else:
                os.environ[nome] = valor
        yield
    finally:
        for nome, valor in antes.items():
            if valor is None:
                os.environ.pop(nome, None)
            else:
                os.environ[nome] = valor


def test_queda_inesperada_reconecta_e_reinicia_os_fluxos():
    async def cenario():
        conexao = ConexaoFalsa(["ok", "ok"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        conexao.derrubar()
        await aguardar(lambda: conexao.conexoes_ok == 2 and conexao.monitores_rssi == 2)

        assert conexao.estado is EstadoConexao.CONECTADO
        assert conexao.assinaturas_fc == 2            # FC reassinada na reconexão
        assert conexao.monitores_rssi_cancelados == 1  # monitor da conexão antiga encerrado

        conexao.derrubar(intencional=True)
        await asyncio.wait_for(tarefa, 2.0)

    asyncio.run(cenario())


def test_desconexao_intencional_nao_reconecta():
    async def cenario():
        conexao = ConexaoFalsa(["ok"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        conexao.derrubar(intencional=True)
        await asyncio.wait_for(tarefa, 2.0)  # o laço termina sozinho, sem erro

        assert conexao.chamadas_conectar == 1
        assert conexao.monitores_rssi_cancelados == 1

    asyncio.run(cenario())


def test_esgota_as_tentativas_e_informa_a_falha():
    async def cenario():
        conexao = ConexaoFalsa(["ok", "falha", "falha", "falha"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        conexao.derrubar()
        await aguardar_erro(tarefa, FalhaConexao)

        assert conexao.chamadas_conectar == 4  # 1 conexão inicial + 3 tentativas
        assert conexao.estado is EstadoConexao.DESCONECTADO

    asyncio.run(cenario())


def test_bluetooth_indisponivel_conta_como_tentativa():
    async def cenario():
        conexao = ConexaoFalsa(["ok", "bluetooth", "ok"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        conexao.derrubar()
        await aguardar(lambda: conexao.conexoes_ok == 2 and conexao.monitores_rssi == 2)
        assert conexao.chamadas_conectar == 3

        conexao.derrubar(intencional=True)
        await asyncio.wait_for(tarefa, 2.0)

    asyncio.run(cenario())


def test_contador_de_tentativas_recomeca_a_cada_queda():
    async def cenario():
        # limite de 2 tentativas por queda: sobrevive a duas quedas seguidas
        conexao = ConexaoFalsa(["ok", "falha", "ok", "falha", "ok"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=2, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        conexao.derrubar()
        await aguardar(lambda: conexao.conexoes_ok == 2 and conexao.monitores_rssi == 2)
        conexao.derrubar()
        await aguardar(lambda: conexao.conexoes_ok == 3 and conexao.monitores_rssi == 3)

        conexao.derrubar(intencional=True)
        await asyncio.wait_for(tarefa, 2.0)

    asyncio.run(cenario())


def test_primeira_conexao_que_falha_nao_e_repetida():
    async def cenario():
        conexao = ConexaoFalsa(["falha"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar_erro(tarefa, FalhaConexao)
        assert conexao.chamadas_conectar == 1

    asyncio.run(cenario())


def test_falha_ao_assinar_a_fc_nao_derruba_o_laco():
    async def cenario():
        conexao = ConexaoFalsa(["ok", "ok"])
        conexao.falhar_assinatura_fc = True
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)  # o RSSI segue mesmo sem a FC

        conexao.derrubar()
        await aguardar(lambda: conexao.conexoes_ok == 2 and conexao.monitores_rssi == 2)
        assert conexao.assinaturas_fc == 2

        conexao.derrubar(intencional=True)
        await asyncio.wait_for(tarefa, 2.0)

    asyncio.run(cenario())


def test_cancelar_o_laco_encerra_o_monitor_de_rssi():
    async def cenario():
        conexao = ConexaoFalsa(["ok"])
        tarefa = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.monitores_rssi == 1)

        tarefa.cancel()
        await asyncio.gather(tarefa, return_exceptions=True)
        assert tarefa.cancelled()
        assert conexao.monitores_rssi_cancelados == 1

    asyncio.run(cenario())


def test_configuracao_por_variavel_de_ambiente():
    with ambiente(RECONEXAO_MAX_TENTATIVAS=None, RECONEXAO_INTERVALO=None):
        assert ler_configuracao_reconexao() == (TENTATIVAS_MAXIMAS_PADRAO, INTERVALO_RECONEXAO_PADRAO)

    with ambiente(RECONEXAO_MAX_TENTATIVAS="7", RECONEXAO_INTERVALO="0.5"):
        assert ler_configuracao_reconexao() == (7, 0.5)
        assert ler_configuracao_reconexao(max_tentativas=2, intervalo=1.0) == (2, 1.0)  # argumento vence


def test_configuracao_invalida_e_rejeitada():
    for max_tentativas, intervalo in ((0, 1.0), (3, -1.0)):
        try:
            ler_configuracao_reconexao(max_tentativas, intervalo)
        except ValueError:
            continue
        raise AssertionError(f"deveria rejeitar ({max_tentativas}, {intervalo})")

    with ambiente(RECONEXAO_MAX_TENTATIVAS="muitas"):
        try:
            ler_configuracao_reconexao()
        except ValueError:
            return
        raise AssertionError("deveria rejeitar variável não numérica")


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
