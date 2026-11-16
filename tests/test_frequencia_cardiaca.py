"""Testes da publicação da frequência cardíaca no broker (RF17), sem broker e sem hardware.

Rodar na raiz do projeto:  python tests/test_frequencia_cardiaca.py
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

from bluetooth.conexao import ConexaoPulseira  # noqa: E402
from bluetooth.domain.erros import BrokerIndisponivel  # noqa: E402
from bluetooth.domain.models import LeituraFrequenciaCardiaca  # noqa: E402
from coletor.coletor import Coletor  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402
from mqtt.frequencia_cardiaca import (  # noqa: E402
    MAX_LEITURAS_SEM_BROKER_PADRAO, FrequenciaCardiacaColetor, ler_max_leituras_sem_broker,
)

PULSEIRA = "A0:9E:1A:00:00:01"
COLETOR = "coletor-a"
OUTRO_COLETOR = "coletor-b"


# ------------------------------------------------------------------ apoio

@contextmanager
def ambiente(**valores: str) -> Iterator[None]:
    anteriores = {nome: os.environ.get(nome) for nome in valores}
    os.environ.update(valores)
    try:
        yield
    finally:
        for nome, valor in anteriores.items():
            if valor is None:
                os.environ.pop(nome, None)
            else:
                os.environ[nome] = valor


@contextmanager
def silenciar_log() -> Iterator[None]:
    raiz = logging.getLogger()
    nivel = raiz.level
    raiz.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        raiz.setLevel(nivel)


class BrokerFalso:
    """Dublê da ConexaoBroker: guarda as publicações (conferindo que passam no de_json) e deixa
    o teste derrubar a sessão, avisando os ganchos de queda como a ConexaoBroker faz."""

    def __init__(self) -> None:
        self.cliente_id = COLETOR
        self.conectado = True
        self.falhar_conectado = False  # Simula erro do aiomqtt com a sessão ainda de pé
        self.ao_receber = None
        self.ultima_vontade = None
        self.keepalive = None
        self.publicadas: list[tuple[str, Mensagem]] = []
        self._ao_desconectar: list[Callable] = []

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    def adicionar_ao_desconectar(self, tratador: Callable) -> None:
        self._ao_desconectar.append(tratador)

    async def assinar(self, _topico: str, qos: int = 1) -> None:
        pass

    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        if not self.conectado or self.falhar_conectado:
            raise BrokerIndisponivel("sem broker (teste)")
        assert Mensagem.de_json(mensagem.para_json(), topico) is not None, f"{topico}: {mensagem}"
        self.publicadas.append((topico, mensagem))
        await asyncio.sleep(0)

    def cair(self) -> None:
        self.conectado = False
        for tratador in self._ao_desconectar:
            tratador()

    def bpms(self) -> list[int]:
        return [m.conteudo["bpm"] for _, m in self.publicadas if m.tipo is TipoMensagem.FREQUENCIA_CARDIACA]


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


async def esperar_para_sempre() -> None:
    await asyncio.sleep(3600)


def leitura(bpm: int, rr: tuple[float, ...] = ()) -> LeituraFrequenciaCardiaca:
    return LeituraFrequenciaCardiaca(bpm=bpm, intervalos_rr_ms=rr)


async def com_publicacao(frequencia: FrequenciaCardiacaColetor) -> asyncio.Task:
    tarefa = asyncio.create_task(frequencia.manter_publicacao())
    await asyncio.sleep(0)
    return tarefa


async def encerrar(*tarefas: asyncio.Task) -> None:
    for tarefa in tarefas:
        tarefa.cancel()
    await asyncio.gather(*tarefas, return_exceptions=True)


def posse(dono: str | None) -> Mensagem:
    return Mensagem(TipoMensagem.POSSE, "coordenador", {"pulseira": PULSEIRA, "coletor": dono})


# ------------------------------------------------------------------ configuração

def test_buffer_padrao_ambiente_e_argumento():
    with ambiente(FC_MAX_LEITURAS_SEM_BROKER=""):
        assert ler_max_leituras_sem_broker() == MAX_LEITURAS_SEM_BROKER_PADRAO == 15
    with ambiente(FC_MAX_LEITURAS_SEM_BROKER="30"):
        assert ler_max_leituras_sem_broker() == 30
        assert ler_max_leituras_sem_broker(4) == 4  # argumento explícito vence o ambiente


def test_buffer_invalido_e_rejeitado():
    for valor in ("0", "-1", "dez", "1.5"):
        with ambiente(FC_MAX_LEITURAS_SEM_BROKER=valor):
            try:
                ler_max_leituras_sem_broker()
            except ValueError as e:
                assert "FC_MAX_LEITURAS_SEM_BROKER" in str(e)
            else:
                raise AssertionError(f"{valor!r} deveria ser rejeitado")


# ------------------------------------------------------------------ mensagem

def test_mensagem_vai_no_topico_da_pulseira_com_os_campos_do_rf15():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        frequencia.receber_posse()
        com_rr, sem_rr = leitura(72, (812.5, 830.078125)), leitura(73)
        frequencia.registrar(com_rr)
        frequencia.registrar(sem_rr)
        await frequencia.publicar_pendentes()

        (topico, primeira), (_, segunda) = broker.publicadas
        assert topico == Topicos.frequencia_cardiaca(PULSEIRA) == f"pulseiras/{PULSEIRA}/frequencia-cardiaca"
        assert primeira.tipo is TipoMensagem.FREQUENCIA_CARDIACA
        assert primeira.origem == COLETOR
        assert primeira.conteudo == {"pulseira": PULSEIRA, "coletor": COLETOR, "bpm": 72, "rr": [812.5, 830.078125]}
        assert segunda.conteudo == {"pulseira": PULSEIRA, "coletor": COLETOR, "bpm": 73}  # sem RR, sem o campo
        # Timestamp do recebimento (RF16), não da publicação: é por ele que o consumidor deduplica
        assert primeira.timestamp == com_rr.timestamp and segunda.timestamp == sem_rr.timestamp
        assert (primeira.config_entrega.qos, primeira.config_entrega.retida) == (0, False)

    asyncio.run(cenario())


# ------------------------------------------------------------------ posse

def test_sem_posse_nao_guarda_nem_publica():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.registrar(leitura(70))
            await asyncio.sleep(0.02)
            assert frequencia.pendentes == 0 and broker.publicadas == []
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_com_posse_publica_na_ordem_em_que_chegaram():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.receber_posse()
            for bpm in (70, 71, 72):
                frequencia.registrar(leitura(bpm))
            await aguardar(lambda: len(broker.publicadas) == 3)
            assert broker.bpms() == [70, 71, 72]
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_perder_a_posse_descarta_as_guardadas_e_ignora_as_atrasadas():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        frequencia.receber_posse()
        broker.cair()
        frequencia.registrar(leitura(70))
        frequencia.registrar(leitura(71))
        with silenciar_log():
            frequencia.perder_posse()
        frequencia.registrar(leitura(72))  # notificação que chega depois de perder a posse
        assert frequencia.pendentes == 0

    asyncio.run(cenario())


# ------------------------------------------------------------------ sem broker

def test_sem_broker_guarda_ate_o_limite_e_publica_em_ordem_quando_a_posse_volta():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA, max_leituras_sem_broker=3)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.receber_posse()
            broker.cair()
            for bpm in (70, 71, 72, 73, 74):
                frequencia.registrar(leitura(bpm))
            await asyncio.sleep(0.02)
            assert frequencia.pendentes == 3 and broker.publicadas == []

            broker.conectado = True
            with silenciar_log():
                frequencia.receber_posse()  # posse retida da reconexão
                await aguardar(lambda: len(broker.publicadas) == 3)
            assert broker.bpms() == [72, 73, 74]  # as duas mais antigas foram descartadas
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_reconexao_espera_a_posse_retida_antes_de_publicar():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.receber_posse()
            broker.cair()
            frequencia.registrar(leitura(70))
            broker.conectado = True  # reconectou, mas a posse retida ainda não chegou
            frequencia.registrar(leitura(71))
            await asyncio.sleep(0.02)
            assert broker.publicadas == []
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_reconexao_sem_a_posse_descarta_as_guardadas():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.receber_posse()
            broker.cair()
            frequencia.registrar(leitura(70))
            broker.conectado = True
            with silenciar_log():
                frequencia.perder_posse()  # posse retida é de outro coletor
            await asyncio.sleep(0.02)
            assert frequencia.pendentes == 0 and broker.publicadas == []
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_queda_no_meio_da_publicacao_devolve_a_leitura_ao_buffer():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        frequencia.receber_posse()
        broker.conectado = False  # caiu, e o gancho de queda ainda não rodou
        frequencia.registrar(leitura(70))
        frequencia.registrar(leitura(71))
        with silenciar_log():
            await frequencia.publicar_pendentes()
        assert frequencia.pendentes == 2  # não perdeu a leitura que falhou

        broker.conectado = True
        frequencia.receber_posse()
        await frequencia.publicar_pendentes()
        assert broker.bpms() == [70, 71]

    asyncio.run(cenario())


def test_falha_com_a_sessao_de_pe_nao_prende_a_publicacao():
    async def cenario():
        broker = BrokerFalso()
        frequencia = FrequenciaCardiacaColetor(broker, PULSEIRA)
        tarefa = await com_publicacao(frequencia)
        try:
            frequencia.receber_posse()
            broker.falhar_conectado = True
            with silenciar_log():
                frequencia.registrar(leitura(70))
                await asyncio.sleep(0.02)
            broker.falhar_conectado = False
            frequencia.registrar(leitura(71))
            await aguardar(lambda: broker.bpms() == [71])  # 70 perdida (QoS 0), 71 segue sem nova posse
        finally:
            await encerrar(tarefa)

    asyncio.run(cenario())


def test_falha_de_quem_publica_nao_interrompe_a_notificacao():
    def quebrado(_leitura: LeituraFrequenciaCardiaca) -> None:
        raise RuntimeError("falha no consumidor")

    conexao = ConexaoPulseira(PULSEIRA)
    conexao.ao_ler_frequencia_cardiaca = quebrado
    with silenciar_log():
        conexao._ao_receber_frequencia_cardiaca(None, bytearray([0x00, 72]))  # flags 0, 72 bpm
        conexao._ao_receber_frequencia_cardiaca(None, bytearray([0x00, 73]))
    assert [l.bpm for l in conexao.registros_frequencia] == [72, 73]


# ------------------------------------------------------------------ integração com o Coletor

def montar_coletor() -> tuple[Coletor, BrokerFalso]:
    broker = BrokerFalso()
    coletor = Coletor(broker, PULSEIRA)

    async def desconectar_falso(reconectar: bool = False) -> None:
        pass

    coletor.varredura.executar = esperar_para_sempre
    coletor._manter_sessao = esperar_para_sempre
    coletor.conexao.desconectar = desconectar_falso
    return coletor, broker


async def desmontar(coletor: Coletor) -> None:
    await coletor._parar_varredura()
    tarefas = [t for t in (coletor._sessao, coletor._sem_broker) if t is not None]
    await encerrar(*tarefas)


def test_coletor_liga_a_conexao_a_publicacao():
    coletor, _ = montar_coletor()
    assert coletor.conexao.ao_ler_frequencia_cardiaca == coletor.frequencia.registrar


def test_posse_de_outro_coletor_descarta_e_ignora_as_atrasadas():
    async def cenario():
        coletor, broker = montar_coletor()
        try:
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            coletor.frequencia.registrar(leitura(70))
            with silenciar_log():
                await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(OUTRO_COLETOR))
            coletor.frequencia.registrar(leitura(71))
            assert coletor.frequencia.pendentes == 0
            assert broker.bpms() == []
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


def test_handoff_publica_as_leituras_antes_da_liberacao():
    async def cenario():
        coletor, broker = montar_coletor()
        try:
            await coletor.ao_receber(Topicos.estado_posse(PULSEIRA), posse(COLETOR))
            coletor.frequencia.registrar(leitura(70))
            coletor.frequencia.registrar(leitura(71))
            handoff = Mensagem(TipoMensagem.HANDOFF, "coordenador", {"id": "ab12", "pulseira": PULSEIRA, "origem": COLETOR})
            with silenciar_log():
                await coletor.ao_receber(Topicos.handoff(PULSEIRA), handoff)

            tipos = [m.tipo for _, m in broker.publicadas]
            assert broker.bpms() == [70, 71]
            ultima_fc = max(i for i, t in enumerate(tipos) if t is TipoMensagem.FREQUENCIA_CARDIACA)
            assert tipos.index(TipoMensagem.LIBERACAO_POSSE) > ultima_fc
            coletor.frequencia.registrar(leitura(72))  # já liberou: não guarda
            assert coletor.frequencia.pendentes == 0
        finally:
            await desmontar(coletor)

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
