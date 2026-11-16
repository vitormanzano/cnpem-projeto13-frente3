"""Testes da conexão com o broker (RF11), sem broker e sem hardware.

O aiomqtt.Client é trocado por um dublê que segue um roteiro de conexões (falha ou sucesso)
e deixa o teste derrubar a sessão quando quiser.

Rodar na raiz do projeto:  python tests/test_broker.py
(também funciona com pytest)
"""
import asyncio
import logging
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

import aiomqtt  # noqa: E402
from bluetooth.domain.erros import BrokerIndisponivel  # noqa: E402
from coletor.coletor import Coletor, ler_prazo_sem_broker  # noqa: E402
from coordenador.coordenador import Coordenador  # noqa: E402
from mqtt.conexao import ConexaoBroker  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402
from mqtt.presenca import PresencaColetor  # noqa: E402

PULSEIRA = "A0:9E:1A:00:00:01"
COLETOR = "coletor-a"
OUTRO_COLETOR = "coletor-b"
VARIAVEIS_BROKER = (
    "BROKER_HOST", "BROKER_PORTA", "BROKER_USUARIO", "BROKER_SENHA",
    "BROKER_INTERVALO_RECONEXAO", "PRAZO_SEM_BROKER",
)


# ------------------------------------------------------------------ apoio

@contextmanager
def ambiente(**valores: str) -> Iterator[None]:
    """Define as variáveis do broker só durante o bloco; as não informadas ficam ausentes."""
    anteriores = {nome: os.environ.pop(nome, None) for nome in VARIAVEIS_BROKER}
    os.environ.update(valores)
    try:
        yield
    finally:
        for nome in VARIAVEIS_BROKER:
            os.environ.pop(nome, None)
        os.environ.update({nome: valor for nome, valor in anteriores.items() if valor is not None})


class CapturaLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.mensagens: list[str] = []

    def emit(self, registro: logging.LogRecord) -> None:
        self.mensagens.append(registro.getMessage())


@contextmanager
def capturar_log() -> Iterator[list[str]]:
    raiz = logging.getLogger()
    captura, nivel = CapturaLog(), raiz.level
    raiz.addHandler(captura)
    raiz.setLevel(logging.DEBUG)
    try:
        yield captura.mensagens
    finally:
        raiz.removeHandler(captura)
        raiz.setLevel(nivel)


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


async def esperar_para_sempre() -> None:
    await asyncio.sleep(3600)


class ClienteMqttFalso:
    """Dublê do aiomqtt.Client. Cada instância é uma tentativa de conexão: o `roteiro` diz se ela
    falha ("falha") ou conecta ("ok", o padrão quando o roteiro acaba); `derrubar()` corta a sessão."""

    roteiro: list[str] = []
    criados: list["ClienteMqttFalso"] = []

    def __init__(self, **parametros: object) -> None:
        self.parametros = parametros
        self.assinados: list[tuple[str, int]] = []
        self._queda = asyncio.Event()
        ClienteMqttFalso.criados.append(self)

    async def __aenter__(self) -> "ClienteMqttFalso":
        if (self.roteiro.pop(0) if self.roteiro else "ok") == "falha":
            raise aiomqtt.MqttError("[code:5] Connection refused")
        return self

    async def __aexit__(self, *_erro: object) -> None:
        return None

    async def subscribe(self, topico: str, qos: int = 0) -> None:
        self.assinados.append((topico, qos))

    def derrubar(self) -> None:
        self._queda.set()

    @property
    def messages(self):
        return self._mensagens()

    async def _mensagens(self):
        await self._queda.wait()
        raise aiomqtt.MqttError("Disconnected during message iteration")
        yield  # torna a função um gerador assíncrono


@contextmanager
def cliente_mqtt_falso(roteiro: list[str]) -> Iterator[list[ClienteMqttFalso]]:
    original = aiomqtt.Client
    ClienteMqttFalso.roteiro = list(roteiro)
    ClienteMqttFalso.criados = []
    aiomqtt.Client = ClienteMqttFalso
    try:
        yield ClienteMqttFalso.criados
    finally:
        aiomqtt.Client = original


class BrokerFalso:
    """Dublê da ConexaoBroker que guarda os ganchos de conexão, para o teste derrubar e
    restabelecer a sessão como a ConexaoBroker faz."""

    def __init__(self) -> None:
        self.cliente_id = COLETOR
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.keepalive = None
        self.publicadas: list[Mensagem] = []
        self._ao_conectar: list[Callable] = []
        self._ao_desconectar: list[Callable] = []

    def adicionar_ao_conectar(self, tratador: Callable) -> None:
        self._ao_conectar.append(tratador)

    def adicionar_ao_desconectar(self, tratador: Callable) -> None:
        self._ao_desconectar.append(tratador)

    async def assinar(self, _topico: str, qos: int = 1) -> None:
        pass

    async def publicar(self, _topico: str, mensagem: Mensagem) -> None:
        if not self.conectado:
            raise BrokerIndisponivel("sem broker (teste)")
        self.publicadas.append(mensagem)
        await asyncio.sleep(0)

    async def cair(self) -> None:
        self.conectado = False
        for tratador in self._ao_desconectar:
            resultado = tratador()
            if asyncio.iscoroutine(resultado):
                await resultado

    async def voltar(self) -> None:
        self.conectado = True
        for tratador in self._ao_conectar:
            resultado = tratador()
            if asyncio.iscoroutine(resultado):
                await resultado


def montar_coletor(prazo: float = 0.05) -> tuple[Coletor, BrokerFalso, dict[str, list]]:
    """Coletor sem BLE: a sessão é um laço vazio e a desconexão só é registrada."""
    broker = BrokerFalso()
    coletor = Coletor(broker, PULSEIRA, prazo_sem_broker=prazo)
    chamadas: dict[str, list] = {"sessoes": [], "desconectar": []}

    async def sessao_falsa() -> None:
        chamadas["sessoes"].append(True)
        coletor._conectou.set()  # a pulseira conectou: o prazo de conexão (RF14) não interfere
        await esperar_para_sempre()

    async def desconectar_falso(reconectar: bool = False) -> None:
        chamadas["desconectar"].append(reconectar)

    coletor.varredura.executar = esperar_para_sempre
    coletor._manter_sessao = sessao_falsa
    coletor.conexao.desconectar = desconectar_falso
    return coletor, broker, chamadas


async def desmontar(coletor: Coletor) -> None:
    await coletor._parar_varredura()
    tarefas = [tarefa for tarefa in (coletor._sessao, coletor._sem_broker) if tarefa is not None]
    for tarefa in tarefas:
        tarefa.cancel()
    await asyncio.gather(*tarefas, return_exceptions=True)


def posse(dono: str | None) -> Mensagem:
    return Mensagem(TipoMensagem.POSSE, "coordenador", {"pulseira": PULSEIRA, "coletor": dono})


# ------------------------------------------------------------------ configuração

def test_configuracao_por_variavel_de_ambiente():
    with ambiente(BROKER_HOST="192.168.0.10", BROKER_PORTA="1884", BROKER_USUARIO="coletor",
                  BROKER_SENHA="segredo", BROKER_INTERVALO_RECONEXAO="2.5"):
        broker = ConexaoBroker(cliente_id=COLETOR)
    assert (broker.host, broker.porta, broker.usuario, broker.senha) == ("192.168.0.10", 1884, "coletor", "segredo")
    assert broker.intervalo_reconexao == 2.5


def test_sem_variaveis_usa_os_padroes_e_credenciais_sao_opcionais():
    with ambiente(BROKER_USUARIO="", BROKER_SENHA=""):
        broker = ConexaoBroker(cliente_id=COLETOR)
    assert (broker.host, broker.porta, broker.intervalo_reconexao) == ("localhost", 1883, 5.0)
    assert broker.usuario is None and broker.senha is None


def test_argumento_explicito_vence_o_ambiente():
    with ambiente(BROKER_HOST="192.168.0.10", BROKER_PORTA="1884", BROKER_INTERVALO_RECONEXAO="9"):
        broker = ConexaoBroker(cliente_id=COLETOR, host="10.0.0.1", porta=1885, intervalo_reconexao=0.5)
    assert (broker.host, broker.porta, broker.intervalo_reconexao) == ("10.0.0.1", 1885, 0.5)


def test_configuracao_invalida_e_rejeitada():
    invalidas = [
        {"BROKER_PORTA": "abc"}, {"BROKER_PORTA": "0"}, {"BROKER_PORTA": "70000"},
        {"BROKER_INTERVALO_RECONEXAO": "rapido"}, {"BROKER_INTERVALO_RECONEXAO": "0"},
        {"BROKER_INTERVALO_RECONEXAO": "-1"},
    ]
    for valores in invalidas:
        with ambiente(**valores):
            try:
                ConexaoBroker(cliente_id=COLETOR)
            except ValueError as e:
                assert next(iter(valores)) in str(e), e
            else:
                raise AssertionError(f"aceitou {valores}")

    for valores in ({"PRAZO_SEM_BROKER": "x"}, {"PRAZO_SEM_BROKER": "0"}):
        with ambiente(**valores):
            try:
                ler_prazo_sem_broker()
            except ValueError as e:
                assert "PRAZO_SEM_BROKER" in str(e), e
            else:
                raise AssertionError(f"aceitou {valores}")


def test_prazo_sem_broker_padrao_e_por_ambiente():
    with ambiente():
        assert ler_prazo_sem_broker() == 15.0
    with ambiente(PRAZO_SEM_BROKER="20"):
        assert ler_prazo_sem_broker() == 20.0
        assert ler_prazo_sem_broker(3) == 3  # argumento vence o ambiente


# ------------------------------------------------------------------ keepalive e sessão

def test_keepalive_segue_o_intervalo_do_sinal_de_vida():
    with ambiente():
        assert ConexaoBroker(cliente_id=COLETOR).keepalive_mqtt == 5

        broker = ConexaoBroker(cliente_id=COLETOR)
        PresencaColetor(broker, intervalo=3)
        assert broker.keepalive_mqtt == 3

        broker = ConexaoBroker(cliente_id=COLETOR)
        PresencaColetor(broker, intervalo=2.5)  # o MQTT só aceita segundos inteiros
        assert broker.keepalive_mqtt == 3

        broker = ConexaoBroker(cliente_id="coordenador")
        Coordenador(broker, intervalo=4, pulseiras=[PULSEIRA])
        assert broker.keepalive_mqtt == 4


def test_conecta_com_client_id_credenciais_keepalive_e_sessao_limpa():
    async def cenario():
        with ambiente(), cliente_mqtt_falso(["ok"]) as clientes:
            broker = ConexaoBroker(cliente_id=COLETOR, host="10.0.0.1", porta=1884,
                                   usuario="u", senha="s", keepalive=4)
            tarefa = asyncio.create_task(broker.manter_conexao())
            try:
                await aguardar(lambda: broker.conectado)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        parametros = clientes[0].parametros
        assert parametros["identifier"] == COLETOR
        assert (parametros["hostname"], parametros["port"]) == ("10.0.0.1", 1884)
        assert (parametros["username"], parametros["password"]) == ("u", "s")
        assert parametros["keepalive"] == 4
        # Os mesmos parâmetros num cliente real: o coletor usa sessão limpa (MQTT 3.1.1)
        assert parametros["clean_session"] is True
        assert aiomqtt.Client(**parametros)._client._clean_session is True

    asyncio.run(cenario())


def test_coordenador_usa_sessao_persistente():
    """As liberações publicadas enquanto o coordenador reinicia ficam guardadas no broker (RF19)."""
    with ambiente():
        broker = ConexaoBroker(cliente_id="coordenador")
        Coordenador(broker, pulseiras=[PULSEIRA])
        assert broker.sessao_persistente
        assert not ConexaoBroker(cliente_id=COLETOR).sessao_persistente

    async def cenario():
        with ambiente(), cliente_mqtt_falso(["ok"]) as clientes:
            persistente = ConexaoBroker(cliente_id="coordenador", sessao_persistente=True)
            tarefa = asyncio.create_task(persistente.manter_conexao())
            try:
                await aguardar(lambda: persistente.conectado)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        parametros = clientes[0].parametros
        assert parametros["clean_session"] is False
        assert aiomqtt.Client(**parametros)._client._clean_session is False

    asyncio.run(cenario())


def test_registra_a_conexao_no_log():
    async def cenario():
        with ambiente(), cliente_mqtt_falso(["ok"]), capturar_log() as log:
            broker = ConexaoBroker(cliente_id=COLETOR)
            tarefa = asyncio.create_task(broker.manter_conexao())
            try:
                await aguardar(lambda: broker.conectado)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)
        assert any("Conectado ao broker localhost:1883" in linha and COLETOR in linha for linha in log), log

    asyncio.run(cenario())


# ------------------------------------------------------------------ reconexão

def test_broker_indisponivel_registra_erro_e_tenta_de_novo_sem_encerrar():
    async def cenario():
        with ambiente(), cliente_mqtt_falso(["falha", "falha", "ok"]) as clientes, capturar_log() as log:
            broker = ConexaoBroker(cliente_id=COLETOR, intervalo_reconexao=0.01)
            tarefa = asyncio.create_task(broker.manter_conexao())
            try:
                await aguardar(lambda: broker.conectado)
                assert not tarefa.done()
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        assert len(clientes) == 3
        erros = [linha for linha in log if BrokerIndisponivel.codigo in linha]
        assert len(erros) == 2 and all("falha ao conectar" in linha for linha in erros), erros
        assert all("nova tentativa em 0.01s" in linha for linha in erros), erros

    asyncio.run(cenario())


def test_reconexao_refaz_as_assinaturas_e_avisa_a_queda_uma_vez():
    async def cenario():
        eventos: list[str] = []
        with ambiente(), cliente_mqtt_falso(["ok", "falha", "ok"]) as clientes, capturar_log() as log:
            broker = ConexaoBroker(
                cliente_id=COLETOR,
                intervalo_reconexao=0.01,
                ao_conectar=lambda: eventos.append("conectou"),
                ao_desconectar=lambda: eventos.append(f"caiu (conectado={broker.conectado})"),
            )
            await broker.assinar("pulseiras/+/posse")
            await broker.assinar("pulseiras/+/handoff", qos=0)
            tarefa = asyncio.create_task(broker.manter_conexao())
            try:
                await aguardar(lambda: broker.conectado)
                clientes[0].derrubar()
                await aguardar(lambda: len(clientes) == 3 and broker.conectado)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        esperadas = [("pulseiras/+/posse", 1), ("pulseiras/+/handoff", 0)]
        assert clientes[0].assinados == esperadas
        assert clientes[2].assinados == esperadas
        # A tentativa que falhou não avisa queda, e o cancelamento (encerramento) também não
        assert eventos == ["conectou", "caiu (conectado=False)", "conectou"], eventos
        assert any("conexão perdida" in linha for linha in log), log

    asyncio.run(cenario())


def test_falha_no_gancho_de_queda_nao_para_a_reconexao():
    async def cenario():
        def quebrado() -> None:
            raise RuntimeError("gancho quebrado")

        with ambiente(), cliente_mqtt_falso(["ok", "ok"]) as clientes, capturar_log():
            broker = ConexaoBroker(cliente_id=COLETOR, intervalo_reconexao=0.01, ao_desconectar=quebrado)
            tarefa = asyncio.create_task(broker.manter_conexao())
            try:
                await aguardar(lambda: broker.conectado)
                clientes[0].derrubar()
                await aguardar(lambda: len(clientes) == 2 and broker.conectado)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

    asyncio.run(cenario())


# ------------------------------------------------------------------ prazo sem broker no coletor

def test_sem_broker_alem_do_prazo_solta_a_pulseira_sem_reconectar():
    async def cenario():
        coletor, broker, chamadas = montar_coletor(prazo=0.05)
        try:
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            await aguardar(lambda: chamadas["sessoes"])
            assert coletor.presenca.pulseira == PULSEIRA

            with capturar_log() as log:
                await broker.cair()
                await asyncio.sleep(0.02)
                assert not chamadas["desconectar"], "soltou antes do prazo"
                await aguardar(lambda: coletor._sessao is None)

            assert chamadas["desconectar"] == [False]  # intencional (RF10): o manter_conexao não reconecta
            assert len(chamadas["sessoes"]) == 1
            assert coletor.presenca.pulseira is None   # o online da reconexão sai sem pulseira
            assert any("sem broker há 0.05s" in linha for linha in log), log

            # Na volta, a posse retida decide: liberada pelo coordenador, volta a varrer
            await broker.voltar()
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(None))
            assert coletor._varredura is not None and not coletor._varredura.done()
            assert len(chamadas["sessoes"]) == 1
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


def test_broker_volta_antes_do_prazo_e_a_pulseira_continua_conectada():
    async def cenario():
        coletor, broker, chamadas = montar_coletor(prazo=0.05)
        try:
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            await aguardar(lambda: chamadas["sessoes"])

            with capturar_log():
                await broker.cair()
                await asyncio.sleep(0.01)
                await broker.voltar()
                await asyncio.sleep(0.1)

            assert not chamadas["desconectar"]
            assert coletor._sessao is not None and not coletor._sessao.done()
            assert coletor._sem_broker is None
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


def test_queda_do_broker_sem_pulseira_nao_inicia_o_prazo():
    async def cenario():
        coletor, broker, chamadas = montar_coletor(prazo=0.01)
        try:
            await broker.cair()
            assert coletor._sem_broker is None
            await asyncio.sleep(0.05)
            assert not chamadas["desconectar"]
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


def test_ao_reconectar_posse_de_outro_coletor_desconecta_da_pulseira():
    async def cenario():
        coletor, broker, chamadas = montar_coletor(prazo=3600)
        try:
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            await aguardar(lambda: chamadas["sessoes"])

            with capturar_log() as log:
                await broker.cair()
                await broker.voltar()
                # Posse retida entregue ao refazer a assinatura: o coordenador deu a pulseira a outro
                await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(OUTRO_COLETOR))

            assert chamadas["desconectar"] == [False]
            assert coletor._sessao is None
            assert coletor._varredura is None  # conectada a outro coletor: não varre
            assert coletor.presenca.pulseira is None
            assert any("Posse perdida sem handoff" in linha for linha in log), log
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


def test_encerramento_cancela_o_prazo_sem_broker():
    async def cenario():
        coletor, broker, chamadas = montar_coletor(prazo=3600)
        coletor.broker.manter_conexao = esperar_para_sempre
        coletor.presenca.manter_sinal_de_vida = esperar_para_sempre
        coletor.estado_rssi.manter_publicacao = esperar_para_sempre
        execucao = asyncio.create_task(coletor.executar())
        try:
            await asyncio.sleep(0)
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            await aguardar(lambda: chamadas["sessoes"])
            with capturar_log():
                await broker.cair()
            prazo = coletor._sem_broker
            assert prazo is not None
        finally:
            execucao.cancel()
            with capturar_log():
                await asyncio.gather(execucao, return_exceptions=True)

        assert prazo.cancelled()
        assert coletor._sem_broker is None

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
