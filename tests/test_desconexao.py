"""Testes da desconexão controlada (RF10), sem hardware e sem broker.

Rodar na raiz do projeto:  python tests/test_desconexao.py
(também funciona com pytest)
"""
import asyncio
import sys
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.conexao import ConexaoPulseira  # noqa: E402
from bluetooth.domain.erros import RSSIIndisponivel  # noqa: E402
from bluetooth.domain.models import EstadoConexao, LeituraRSSI  # noqa: E402
from bluetooth.reconexao import manter_conexao  # noqa: E402
from coletor.coletor import Coletor  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402

ENDERECO = "A0:9E:1A:00:00:01"
COLETOR = "coletor-a"


class BrokerFalso:
    """Dublê da ConexaoBroker: guarda o tipo (e o estado, se houver) de cada publicação, na ordem."""

    def __init__(self) -> None:
        self.cliente_id = COLETOR
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.publicadas: list[str] = []

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    async def assinar(self, _topico: str, qos: int = 1) -> None:
        pass

    async def publicar(self, _topico: str, mensagem: Mensagem) -> None:
        # Como no paho, a mensagem sai na chamada; o await é a espera do PUBACK
        estado = mensagem.conteudo.get("estado")
        self.publicadas.append(f"{mensagem.tipo.value}:{estado}" if estado else mensagem.tipo.value)
        await asyncio.sleep(0.01)

    async def manter_conexao(self) -> None:
        await asyncio.sleep(3600)


class ClienteBleFalso:
    """Dublê do BleakClient: o disconnect leva um tempo, como o real, e avisa pelo callback."""

    def __init__(self, conexao: ConexaoPulseira) -> None:
        self.is_connected = True
        self._conexao = conexao

    async def start_notify(self, *_args) -> None:
        pass

    async def disconnect(self) -> None:
        await asyncio.sleep(0.05)
        self.is_connected = False
        self._conexao._ao_desconectar(self)


def preparar_conexao(conexao: ConexaoPulseira, falhar_rssi_ao_desfazer: bool = False) -> dict[str, int]:
    """Troca o BLE da conexão por dublês e devolve o contador de chamadas a conectar().

    Com `falhar_rssi_ao_desfazer`, o btmgmt responde "Not Connected" enquanto o enlace é desfeito.
    """
    chamadas = {"conectar": 0}
    conexao.intervalo_rssi = 0.01

    async def conectar() -> None:
        chamadas["conectar"] += 1
        conexao._encerramento_solicitado = False
        conexao._motivo_desconexao = None
        conexao._desconectado.clear()
        conexao._client = ClienteBleFalso(conexao)
        conexao._mudar_estado(EstadoConexao.CONECTADO)

    async def ler_rssi() -> LeituraRSSI:
        if falhar_rssi_ao_desfazer and conexao.desconexao_intencional:
            raise RSSIIndisponivel("Not Connected")
        leitura = LeituraRSSI(conexao.endereco, EstadoConexao.CONECTADO, -70)
        conexao._notificar(conexao.ao_atualizar, leitura)
        return leitura

    conexao.conectar = conectar
    conexao.ler_rssi = ler_rssi
    return chamadas


def montar_coletor() -> tuple[Coletor, BrokerFalso, dict[str, int]]:
    broker = BrokerFalso()
    coletor = Coletor(broker, ENDERECO)
    chamadas = preparar_conexao(coletor.conexao)

    async def varrer() -> None:
        await asyncio.sleep(3600)

    coletor.varredura.executar = varrer
    return coletor, broker, chamadas


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


async def receber_posse(coletor: Coletor) -> None:
    posse = Mensagem(TipoMensagem.POSSE, "coordenador", {"pulseira": ENDERECO, "coletor": COLETOR})
    await coletor.ao_receber(Topicos.estado_posse(ENDERECO), posse)
    await aguardar(lambda: coletor.conexao.conectado)
    await asyncio.sleep(0.05)  # algumas leituras de RSSI "conectado" na fila


def test_handoff_publica_desconectado_antes_da_liberacao_e_volta_a_varrer():
    async def cenario():
        coletor, broker, chamadas = montar_coletor()
        principal = asyncio.create_task(coletor.executar())
        try:
            await receber_posse(coletor)
            handoff = Mensagem(TipoMensagem.HANDOFF, "coordenador", {"pulseira": ENDERECO, "origem": COLETOR})
            await coletor.ao_receber(Topicos.handoff(ENDERECO), handoff)
            await asyncio.sleep(0.1)  # tempo para uma reconexão indevida aparecer

            assert broker.publicadas[-2:] == ["estado-pulseira:desconectado", "liberacao-posse"], broker.publicadas
            assert coletor.estado is EstadoConexao.DESCONECTADO
            assert chamadas["conectar"] == 1  # sem reconexão automática
            assert coletor._varredura is not None and not coletor._varredura.done()  # volta a disputar
        finally:
            principal.cancel()
            await asyncio.gather(principal, return_exceptions=True)

    asyncio.run(cenario())


def test_encerramento_desconecta_libera_e_por_fim_anuncia_offline():
    async def cenario():
        coletor, broker, _ = montar_coletor()
        principal = asyncio.create_task(coletor.executar())
        await receber_posse(coletor)

        principal.cancel()  # o que o SIGINT/SIGTERM faz em app.principal
        await asyncio.gather(principal, return_exceptions=True)

        assert broker.publicadas[-3:] == [
            "estado-pulseira:desconectado", "liberacao-posse", "presenca:offline",
        ], broker.publicadas
        assert not coletor.conexao.conectado
        assert coletor._varredura is None  # nenhuma varredura sobra depois do encerramento

    asyncio.run(cenario())


def test_rssi_esgotado_durante_a_desconexao_intencional_nao_reconecta():
    async def cenario():
        conexao = ConexaoPulseira(ENDERECO, falhas_maximas_rssi=1)
        chamadas = preparar_conexao(conexao, falhar_rssi_ao_desfazer=True)
        laco = asyncio.create_task(manter_conexao(conexao, max_tentativas=3, intervalo=0))
        await aguardar(lambda: conexao.conectado)

        await conexao.desconectar()
        await asyncio.wait_for(laco, 1.0)  # o laço termina sozinho, sem reconectar

        assert chamadas["conectar"] == 1
        assert conexao.estado is EstadoConexao.DESCONECTADO

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
