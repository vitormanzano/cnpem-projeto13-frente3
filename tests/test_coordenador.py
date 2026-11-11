"""Testes da visão que o coordenador monta das mensagens dos coletores (RF13), sem broker e sem hardware.

Rodar na raiz do projeto:  python tests/test_coordenador.py
(também funciona com pytest)
"""
import asyncio
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.domain.models import EstadoConexao  # noqa: E402
from coletor.coletor import Coletor, ler_prazo_conexao  # noqa: E402
from coordenador.coordenador import (  # noqa: E402
    PRAZO_SINAL_DE_VIDA_PADRAO, Coordenador, ler_fator_extensao, ler_janela_disputa, ler_margem_histerese,
    ler_minimo_amostras, ler_prazo_sinal_de_vida, ler_pulseiras_acompanhadas,
)
from coordenador.decisao import escolher_coletor  # noqa: E402
from coordenador.visao import Candidato, VisaoColetores  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402

PULSEIRA = "A0:9E:1A:00:00:01"
NAO_CONFIGURADA = "A0:9E:1A:00:00:99"
COLETOR_A = "coletor-a"
COLETOR_B = "coletor-b"
COORDENADOR = "coordenador"
PRAZO = 15.0
INICIO = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------------ apoio

class BrokerFalso:
    """Dublê da ConexaoBroker: guarda as assinaturas e as publicações."""

    def __init__(self, cliente_id: str = COORDENADOR) -> None:
        self.cliente_id = cliente_id
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.keepalive = None
        self.assinaturas: list[tuple[str, int]] = []
        self.publicadas: list[tuple[str, Mensagem]] = []

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    def adicionar_ao_desconectar(self, _tratador: Callable) -> None:
        pass

    async def assinar(self, topico: str, qos: int = 1) -> None:
        self.assinaturas.append((topico, qos))

    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        self.publicadas.append((topico, mensagem))

    def posses(self) -> list[dict]:
        return [mensagem.conteudo for _, mensagem in self.publicadas if mensagem.tipo is TipoMensagem.POSSE]


class CapturaLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.registros: list[logging.LogRecord] = []

    def emit(self, registro: logging.LogRecord) -> None:
        self.registros.append(registro)


@contextmanager
def capturar_log() -> Iterator[list[logging.LogRecord]]:
    raiz = logging.getLogger()
    captura, nivel = CapturaLog(), raiz.level
    raiz.addHandler(captura)
    raiz.setLevel(logging.DEBUG)
    try:
        yield captura.registros
    finally:
        raiz.removeHandler(captura)
        raiz.setLevel(nivel)


@contextmanager
def ambiente(**valores: str) -> Iterator[None]:
    """Define as variáveis só durante o bloco; as não informadas do coordenador ficam ausentes."""
    nomes = {"PULSEIRAS", "TARGET_ADDRESS", "PRAZO_SINAL_DE_VIDA_COLETOR"} | valores.keys()
    anteriores = {nome: os.environ.pop(nome, None) for nome in nomes}
    os.environ.update(valores)
    try:
        yield
    finally:
        for nome in nomes:
            os.environ.pop(nome, None)
        os.environ.update({nome: valor for nome, valor in anteriores.items() if valor is not None})


def ts(segundos: float) -> datetime:
    return INICIO + timedelta(seconds=segundos)


def presenca(coletor: str, estado: str, t: float, pulseira: str | None = None) -> tuple[str, Mensagem]:
    conteudo = {"estado": estado, "pulseira": pulseira}
    return Topicos.presenca_coletor(coletor), Mensagem(TipoMensagem.PRESENCA, coletor, conteudo, timestamp=ts(t))


def sinal_de_vida(coletor: str, t: float) -> tuple[str, Mensagem]:
    return Topicos.sinal_de_vida_coletor(coletor), Mensagem(TipoMensagem.SINAL_DE_VIDA, coletor, {}, timestamp=ts(t))


def estado_rssi(
    coletor: str, t: float, rssi: object = -60, estado: str = "conectado", pulseira: str = PULSEIRA,
    amostras: int | None = None,
) -> tuple[str, Mensagem]:
    conteudo = {"pulseira": pulseira, "estado": estado, "rssi": rssi}
    if amostras is not None:  # só a varredura publica amostras (RF18)
        conteudo["amostras"] = amostras
    return Topicos.estado_rssi(coletor, pulseira), Mensagem(TipoMensagem.ESTADO_PULSEIRA, coletor, conteudo, timestamp=ts(t))


def liberacao(coletor: str, t: float, pulseira: str = PULSEIRA) -> tuple[str, Mensagem]:
    return Topicos.liberacao_posse(pulseira), Mensagem(TipoMensagem.LIBERACAO_POSSE, coletor, {"pulseira": pulseira}, timestamp=ts(t))


def posse(dono: object, pulseira: str = PULSEIRA) -> tuple[str, Mensagem]:
    return Topicos.estado_posse(pulseira), Mensagem(TipoMensagem.POSSE, COORDENADOR, {"pulseira": pulseira, "coletor": dono})


def montar() -> tuple[Coordenador, BrokerFalso]:
    broker = BrokerFalso()
    coordenador = Coordenador(broker, pulseiras=[PULSEIRA], prazo_sinal_de_vida=PRAZO)
    coordenador.janela_disputa = 3600  # nenhuma disputa termina durante o teste
    return coordenador, broker


async def entregar(coordenador: Coordenador, *mensagens: tuple[str, Mensagem]) -> None:
    for topico, mensagem in mensagens:
        await coordenador.ao_receber(topico, mensagem)


async def desmontar(coordenador: Coordenador) -> None:
    tarefas = list(coordenador._disputas.values())
    for tarefa in tarefas:
        tarefa.cancel()
    await asyncio.gather(*tarefas, return_exceptions=True)


# ------------------------------------------------------------------ assinaturas

def test_coordenador_assina_presenca_sinal_de_vida_estado_e_liberacao_de_todos():
    coordenador, broker = montar()
    asyncio.run(coordenador.assinar_topicos())

    assert broker.assinaturas == [
        ("coletores/+/presenca", 1),
        ("coletores/+/sinal-de-vida", 0),
        ("coletores/+/pulseiras/+", 0),
        ("pulseiras/+/posse", 1),
        ("pulseiras/+/liberacao", 1),
    ]


def test_coletor_assina_o_coordenador_e_os_topicos_da_sua_pulseira():
    broker = BrokerFalso(COLETOR_A)
    asyncio.run(Coletor(broker, PULSEIRA).assinar_topicos())

    assert broker.assinaturas == [
        (f"pulseiras/{PULSEIRA}/posse", 1),
        (f"pulseiras/{PULSEIRA}/handoff", 1),
        ("coordenador/presenca", 1),
        ("coordenador/sinal-de-vida", 0),
    ]


def test_extrair_sinal_de_vida_so_do_proprio_topico():
    assert Topicos.extrair_sinal_de_vida_coletor(Topicos.sinal_de_vida_coletor(COLETOR_A)) == COLETOR_A
    assert Topicos.extrair_sinal_de_vida_coletor(Topicos.presenca_coletor(COLETOR_A)) is None
    assert Topicos.extrair_sinal_de_vida_coletor(Topicos.sinal_de_vida_coordenador()) is None


# ------------------------------------------------------------------ configuração

def test_pulseiras_vem_de_pulseiras_ou_do_target_address():
    with ambiente(PULSEIRAS=f" {PULSEIRA.lower()}, {NAO_CONFIGURADA} ,"):
        assert ler_pulseiras_acompanhadas() == {PULSEIRA, NAO_CONFIGURADA}
    with ambiente(TARGET_ADDRESS=PULSEIRA):
        assert ler_pulseiras_acompanhadas() == {PULSEIRA}
    with ambiente(PULSEIRAS=PULSEIRA, TARGET_ADDRESS=NAO_CONFIGURADA):
        assert ler_pulseiras_acompanhadas() == {PULSEIRA}
    with ambiente(PULSEIRAS=NAO_CONFIGURADA):
        assert ler_pulseiras_acompanhadas([PULSEIRA]) == {PULSEIRA}  # argumento vence o ambiente


def test_configuracao_invalida_falha_na_criacao():
    casos = {
        "sem pulseiras": ({}, {}),
        "lista vazia": ({}, {"pulseiras": []}),
        "MAC inválido": ({"PULSEIRAS": "nao-e-mac"}, {}),
        "prazo zero": ({}, {"pulseiras": [PULSEIRA], "prazo_sinal_de_vida": 0}),
        "prazo texto": ({"PRAZO_SINAL_DE_VIDA_COLETOR": "quinze"}, {"pulseiras": [PULSEIRA]}),
    }
    for nome, (variaveis, argumentos) in casos.items():
        with ambiente(**variaveis):
            try:
                Coordenador(BrokerFalso(), **argumentos)
            except ValueError:
                continue
        raise AssertionError(f"{nome}: aceitou configuração inválida")


def test_prazo_de_sinal_de_vida_padrao_e_por_ambiente():
    with ambiente():
        assert ler_prazo_sinal_de_vida() == PRAZO_SINAL_DE_VIDA_PADRAO == 15.0
    with ambiente(PRAZO_SINAL_DE_VIDA_COLETOR="20"):
        assert ler_prazo_sinal_de_vida() == 20.0
        assert ler_prazo_sinal_de_vida(7) == 7


# ------------------------------------------------------------------ pulseiras não configuradas

def test_pulseira_nao_configurada_e_ignorada_e_registrada_em_log():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 0))
            with capturar_log() as log:
                for t in (1, 2, 3):
                    await entregar(coordenador, estado_rssi(COLETOR_A, t, pulseira=NAO_CONFIGURADA))
                await entregar(coordenador, posse(COLETOR_A, NAO_CONFIGURADA), liberacao(COLETOR_A, 4, NAO_CONFIGURADA))

            assert NAO_CONFIGURADA not in coordenador.visao.sinais
            assert NAO_CONFIGURADA not in coordenador.visao.posse
            assert not coordenador._disputas and not broker.publicadas
            avisos = [r for r in log if NAO_CONFIGURADA in r.getMessage() and "não configurada" in r.getMessage()]
            assert len(avisos) == 5, [r.getMessage() for r in log]
            # Só a primeira em WARNING: o RSSI chega a cada segundo
            assert [r.levelno for r in avisos].count(logging.WARNING) == 1
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


# ------------------------------------------------------------------ visão por coletor

def test_visao_guarda_presenca_pulseira_estado_rssi_e_horario_por_coletor():
    async def cenario():
        coordenador, _ = montar()
        try:
            antes = time.monotonic()
            await entregar(
                coordenador,
                presenca(COLETOR_A, "online", 0, pulseira=PULSEIRA),
                estado_rssi(COLETOR_A, 1, rssi=-70),
                estado_rssi(COLETOR_A, 2, rssi=-65),
                presenca(COLETOR_B, "online", 0),
                estado_rssi(COLETOR_B, 1, rssi=None, estado="nao-encontrado"),
            )
            visao = coordenador.visao

            assert visao.disponivel(COLETOR_A) and visao.disponivel(COLETOR_B)
            assert visao.pulseira_atendida == {COLETOR_A: PULSEIRA, COLETOR_B: None}
            assert visao.sinais[PULSEIRA][COLETOR_A].estado is EstadoConexao.CONECTADO
            assert visao.sinais[PULSEIRA][COLETOR_A].rssi == -65
            assert visao.sinais[PULSEIRA][COLETOR_B].estado is EstadoConexao.NAO_ENCONTRADO
            assert visao.sinais[PULSEIRA][COLETOR_B].rssi is None
            assert visao.ultimo_timestamp == {COLETOR_A: ts(2), COLETOR_B: ts(1)}
            assert all(visao.ultima_mensagem[c] >= antes for c in (COLETOR_A, COLETOR_B))
            assert f"Coletor {COLETOR_A}" in str(visao)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


# ------------------------------------------------------------------ indisponibilidade

def test_coletor_sem_sinal_de_vida_alem_do_prazo_fica_indisponivel_e_volta_com_nova_mensagem():
    visao = VisaoColetores()
    visao.reiniciar_contagem(agora=0)
    visao.atualizar_presenca(COLETOR_A, True)
    visao.registrar_mensagem(COLETOR_A, ts(0), agora=0)
    visao.registrar_mensagem(COLETOR_A, ts(10), agora=10)  # o sinal de vida renova o prazo

    assert visao.expirar_sem_sinal(PRAZO, agora=10 + PRAZO) == []  # exatamente no prazo ainda vale
    assert visao.expirar_sem_sinal(PRAZO, agora=10 + PRAZO + 0.1) == [COLETOR_A]
    assert not visao.disponivel(COLETOR_A)
    assert visao.expirar_sem_sinal(PRAZO, agora=100) == []  # não avisa de novo

    visao.registrar_mensagem(COLETOR_A, ts(30), agora=101)
    assert visao.disponivel(COLETOR_A)


def test_coletor_sem_sinal_de_vida_perde_a_posse_e_nao_entra_em_disputa():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(
                coordenador, presenca(COLETOR_A, "online", 0), posse(COLETOR_A),
                estado_rssi(COLETOR_A, 1, estado="desconectado", amostras=10),
            )
            assert coordenador.visao.candidatos(PULSEIRA, desde=0)

            with capturar_log() as log:
                await coordenador._verificar_coletores(agora=time.monotonic() + PRAZO + 1)

            assert not coordenador.visao.disponivel(COLETOR_A)
            assert broker.posses() == [{"pulseira": PULSEIRA, "coletor": None}]
            assert not coordenador.visao.candidatos(PULSEIRA, desde=0)
            assert any(r.levelno == logging.WARNING and "Coletor indisponível" in r.getMessage() for r in log)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_presenca_offline_torna_indisponivel_e_libera_a_posse():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 0, PULSEIRA), posse(COLETOR_A))
            await entregar(coordenador, presenca(COLETOR_A, "offline", 5))

            assert not coordenador.visao.disponivel(COLETOR_A)
            assert broker.posses() == [{"pulseira": PULSEIRA, "coletor": None}]
            assert coordenador.visao.pulseira_atendida[COLETOR_A] is None
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_sem_broker_a_contagem_para_e_recomeca_na_reconexao():
    async def cenario():
        coordenador, broker = montar()
        try:
            coordenador._reiniciar_contagem()
            await entregar(coordenador, presenca(COLETOR_A, "online", 0), posse(COLETOR_A))
            coordenador._parar_contagem()
            assert not coordenador._ouvindo  # o laço de verificação fica parado

            coordenador.visao.ultima_mensagem[COLETOR_A] -= PRAZO * 2  # simula o tempo sem broker
            coordenador._reiniciar_contagem()
            await coordenador._verificar_coletores()

            assert coordenador._ouvindo and coordenador.visao.disponivel(COLETOR_A)
            assert not broker.posses()
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


# ------------------------------------------------------------------ presença x posse

def test_divergencia_entre_presenca_e_posse_vale_a_posse_com_o_dono_online():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_B, "online", 0), posse(COLETOR_B))
            with capturar_log() as log:
                await entregar(coordenador, presenca(COLETOR_A, "online", 0, pulseira=PULSEIRA))
                await coordenador._verificar_coletores(agora=time.monotonic() + 1)

            assert coordenador.visao.posse[PULSEIRA] == COLETOR_B
            assert not broker.posses()
            assert any("Presença diverge da posse" in r.getMessage() and COLETOR_A in r.getMessage() for r in log)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_posse_de_coletor_sem_presenca_conhecida_e_liberada_depois_do_prazo():
    async def cenario():
        coordenador, broker = montar()
        try:
            coordenador._reiniciar_contagem()
            await entregar(coordenador, posse(COLETOR_A))  # a posse retida pode chegar antes da presença

            await coordenador._verificar_coletores(agora=time.monotonic() + PRAZO - 1)
            assert not broker.posses()

            await coordenador._verificar_coletores(agora=time.monotonic() + PRAZO + 1)
            assert broker.posses() == [{"pulseira": PULSEIRA, "coletor": None}]
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


# ------------------------------------------------------------------ formato

def test_conteudo_invalido_e_descartado_com_log_sem_excecao():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 0), posse(COLETOR_A))
            _, outra_origem = presenca(COLETOR_B, "offline", 1)
            casos = {
                "presença com estado desconhecido": presenca(COLETOR_A, "banana", 1),
                "presença com pulseira numérica": presenca(COLETOR_A, "online", 1, pulseira=42),
                "presença com outra origem": (Topicos.presenca_coletor(COLETOR_A), outra_origem),
                "sinal de vida com outra origem": (Topicos.sinal_de_vida_coletor(COLETOR_A), sinal_de_vida(COLETOR_B, 1)[1]),
                "rssi booleano": estado_rssi(COLETOR_A, 1, rssi=True),
                "rssi texto": estado_rssi(COLETOR_A, 1, rssi="-60"),
                "estado desconhecido": estado_rssi(COLETOR_A, 1, estado="perdido"),
                "estado com outra origem": (Topicos.estado_rssi(COLETOR_A, PULSEIRA), estado_rssi(COLETOR_B, 1)[1]),
                "pulseira do conteúdo diferente do tópico": (
                    Topicos.estado_rssi(COLETOR_A, PULSEIRA), estado_rssi(COLETOR_A, 1, pulseira=NAO_CONFIGURADA)[1],
                ),
                "posse com coletor lista": posse(["x"]),
                "posse com coletor vazio": posse(""),
                "liberação com pulseira diferente do tópico": (
                    Topicos.liberacao_posse(PULSEIRA), liberacao(COLETOR_A, 1, NAO_CONFIGURADA)[1],
                ),
            }
            for nome, (topico, mensagem) in casos.items():
                with capturar_log() as log:
                    await coordenador.ao_receber(topico, mensagem)  # não pode levantar
                assert any(r.levelno == logging.WARNING for r in log), f"{nome}: sem aviso no log"

            visao = coordenador.visao
            assert visao.disponivel(COLETOR_A), "uma presença inválida mudou o coletor"
            assert visao.posse[PULSEIRA] == COLETOR_A and not broker.posses()
            assert COLETOR_A not in visao.sinais.get(PULSEIRA, {})
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


# ------------------------------------------------------------------ ordem por timestamp

def test_mensagem_mais_antiga_do_mesmo_coletor_e_ignorada_em_qualquer_topico():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 0), posse(COLETOR_A))
            await entregar(coordenador, estado_rssi(COLETOR_A, 100, rssi=-60))

            with capturar_log():
                await entregar(
                    coordenador,
                    estado_rssi(COLETOR_A, 50, rssi=-90),               # mesmo tópico
                    sinal_de_vida(COLETOR_A, 60),                       # outro tópico
                    presenca(COLETOR_A, "online", 70, pulseira="X"),    # outro tópico
                    liberacao(COLETOR_A, 80),                           # tópico da pulseira, origem do coletor
                )

            visao = coordenador.visao
            assert visao.sinais[PULSEIRA][COLETOR_A].rssi == -60
            assert visao.pulseira_atendida[COLETOR_A] is None
            assert visao.ultimo_timestamp[COLETOR_A] == ts(100)
            assert visao.posse[PULSEIRA] == COLETOR_A and not broker.posses()

            # Mesmo timestamp não é "mais antigo"; o de outro coletor não é comparado
            await entregar(coordenador, estado_rssi(COLETOR_A, 100, rssi=-55), presenca(COLETOR_B, "online", 1))
            assert visao.sinais[PULSEIRA][COLETOR_A].rssi == -55
            assert visao.disponivel(COLETOR_B)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_offline_da_ultima_vontade_vale_mesmo_mais_antigo_e_recomeca_a_ordem():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 10), posse(COLETOR_A), sinal_de_vida(COLETOR_A, 100))
            # A última vontade leva o timestamp de quando foi registrada, antes do online
            await entregar(coordenador, presenca(COLETOR_A, "offline", 0))
            assert not coordenador.visao.disponivel(COLETOR_A)
            assert broker.posses() == [{"pulseira": PULSEIRA, "coletor": None}]

            # Reiniciado com o relógio atrás (Raspberry sem RTC): a nova sessão é aceita
            await entregar(coordenador, presenca(COLETOR_A, "online", 5))
            assert coordenador.visao.disponivel(COLETOR_A)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())

# ------------------------------------------------------------------ disputa pela posse (RF14)

def varredura(coletor: str, t: float, rssi: int, amostras: int = 10) -> tuple[str, Mensagem]:
    """Mediana publicada pela varredura (RF18), com a posse liberada."""
    return estado_rssi(coletor, t, rssi=rssi, estado="desconectado", amostras=amostras)


def montar_disputa(janela: float = 0.05, prazo_conexao: float = 0.1) -> tuple[Coordenador, BrokerFalso]:
    """Coordenador com janela e prazo de conexão curtos, para a disputa terminar durante o teste."""
    coordenador, broker = montar()
    coordenador.janela_disputa = janela
    coordenador.prazo_conexao = prazo_conexao
    return coordenador, broker


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


def donos(broker: BrokerFalso) -> list[str | None]:
    return [conteudo["coletor"] for conteudo in broker.posses()]


def test_configuracao_da_disputa_padrao_por_ambiente_e_invalida():
    vazias = dict(JANELA_DISPUTA="", FATOR_EXTENSAO_JANELA="", MARGEM_HISTERESE="", MINIMO_AMOSTRAS="", PRAZO_CONEXAO="")
    with ambiente(**vazias):
        assert (ler_janela_disputa(), ler_fator_extensao(), ler_margem_histerese()) == (3.0, 3.0, 5.0)
        assert (ler_minimo_amostras(), ler_prazo_conexao()) == (5, 5.0)
    with ambiente(JANELA_DISPUTA="2", MINIMO_AMOSTRAS="4", PRAZO_CONEXAO="7"):
        assert (ler_janela_disputa(), ler_minimo_amostras(), ler_prazo_conexao()) == (2.0, 4, 7.0)
        assert ler_janela_disputa(1.5) == 1.5  # argumento explícito vence o ambiente

    invalidas = [
        lambda: ler_janela_disputa(0), lambda: ler_fator_extensao(0.5), lambda: ler_margem_histerese(-1),
        lambda: ler_minimo_amostras(0), lambda: ler_prazo_conexao(0),
    ]
    for ler in invalidas:
        try:
            ler()
        except ValueError:
            continue
        raise AssertionError("configuração inválida aceita")
    with ambiente(MINIMO_AMOSTRAS="cinco"):
        try:
            ler_minimo_amostras()
        except ValueError as e:
            assert "MINIMO_AMOSTRAS" in str(e)
        else:
            raise AssertionError("MINIMO_AMOSTRAS não numérico aceito")


def test_vence_o_maior_rssi_suavizado():
    candidatos = {COLETOR_A: Candidato(-70.0, 10, 1.0), COLETOR_B: Candidato(-60.0, 10, 2.0)}
    assert escolher_coletor(candidatos, favorito=None, margem=5) == COLETOR_B
    assert escolher_coletor({}, favorito=COLETOR_A) is None


def test_empate_vence_o_favorito_e_sem_ele_quem_publicou_primeiro():
    candidatos = {COLETOR_A: Candidato(-60.0, 10, 2.0), COLETOR_B: Candidato(-60.0, 10, 1.0)}
    assert escolher_coletor(candidatos, favorito=COLETOR_A, margem=0) == COLETOR_A
    assert escolher_coletor(candidatos, favorito=None, margem=0) == COLETOR_B  # chegou primeiro, não o menor id
    tres = candidatos | {"coletor-c": Candidato(-80.0, 10, 0.5)}
    assert escolher_coletor(tres, favorito="coletor-c", margem=0) == COLETOR_B  # favorito fora do empate


def test_favorito_so_perde_pela_margem_e_sem_sinal_perde_sem_margem():
    favorito = Candidato(-70.0, 10, 1.0)
    assert escolher_coletor({COLETOR_A: favorito, COLETOR_B: Candidato(-66.0, 10, 2.0)}, COLETOR_A, margem=5) == COLETOR_A
    assert escolher_coletor({COLETOR_A: favorito, COLETOR_B: Candidato(-65.0, 10, 2.0)}, COLETOR_A, margem=5) == COLETOR_B
    # Sem sinal válido na janela, o favorito não protege nada
    assert escolher_coletor({COLETOR_B: Candidato(-90.0, 10, 2.0)}, COLETOR_A, margem=5) == COLETOR_B


def test_rssi_suavizado_pondera_so_as_medianas_validas_da_janela():
    visao = VisaoColetores(minimo_amostras=5)
    visao.atualizar_presenca(COLETOR_A, True)
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.DESCONECTADO, -40, 20)  # antes da janela
    inicio = time.monotonic()
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.DESCONECTADO, -60, 6)
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.DESCONECTADO, -70, 3)   # poucas amostras
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.DESCONECTADO, -80, 12)
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.NAO_ENCONTRADO, None)  # não apaga as medianas
    visao.registrar_sinal(PULSEIRA, COLETOR_A, EstadoConexao.CONECTADO, -50)        # conexão ativa não é mediana

    candidato = visao.candidatos(PULSEIRA, inicio)[COLETOR_A]
    assert abs(candidato.rssi_suavizado - (-60 * 6 + -80 * 12) / 18) < 1e-9
    assert candidato.amostras == 18 and candidato.primeira_em >= inicio

    visao.atualizar_presenca(COLETOR_A, False)
    assert visao.candidatos(PULSEIRA, inicio) == {}  # coletor offline não é candidato


def test_disputa_so_abre_com_sinal_valido_de_coletor_online():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, posse(None), varredura(COLETOR_A, 1, -60))  # ainda sem presença
            await entregar(coordenador, presenca(COLETOR_A, "online", 2), varredura(COLETOR_A, 3, -60, amostras=4))
            await entregar(coordenador, estado_rssi(COLETOR_A, 4, rssi=-60, estado="desconectado"))  # sem amostras
            assert PULSEIRA not in coordenador._disputas

            with capturar_log() as log:
                await entregar(coordenador, varredura(COLETOR_A, 5, -60, amostras=5))
            assert PULSEIRA in coordenador._disputas
            assert any("Disputa aberta" in r.getMessage() and r.created for r in log)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_disputa_concede_ao_melhor_e_a_posse_fica_quando_ele_conecta_no_prazo():
    async def cenario():
        coordenador, broker = montar_disputa()
        try:
            await entregar(coordenador, posse(None), presenca(COLETOR_A, "online", 0), presenca(COLETOR_B, "online", 0))
            with capturar_log() as log:
                await entregar(coordenador, varredura(COLETOR_A, 1, -70), varredura(COLETOR_B, 1, -60))
                await aguardar(lambda: donos(broker) == [COLETOR_B])
                await entregar(coordenador, estado_rssi(COLETOR_B, 2, rssi=-60))  # conectado
                await aguardar(lambda: PULSEIRA not in coordenador._disputas)
                await asyncio.sleep(coordenador.prazo_conexao * 1.5)

            assert donos(broker) == [COLETOR_B]
            assert coordenador._ultimo_ativo[PULSEIRA] == COLETOR_B
            concessao = next(r.getMessage() for r in log if "Posse concedida" in r.getMessage())
            assert COLETOR_A in concessao and COLETOR_B in concessao  # RSSI de cada candidato
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_vencedor_que_nao_conecta_no_prazo_perde_a_posse():
    async def cenario():
        coordenador, broker = montar_disputa()
        try:
            await entregar(coordenador, posse(None), presenca(COLETOR_A, "online", 0))
            with capturar_log() as log:
                await entregar(coordenador, varredura(COLETOR_A, 1, -60))
                await aguardar(lambda: donos(broker) == [COLETOR_A, None])
            assert any("Posse liberada" in r.getMessage() and "não conectou" in r.getMessage() for r in log), log
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_vencedor_que_libera_antes_de_conectar_perde_a_posse_sem_esperar_o_prazo():
    async def cenario():
        coordenador, broker = montar_disputa(prazo_conexao=3600)
        try:
            await entregar(coordenador, posse(None), presenca(COLETOR_A, "online", 0))
            await entregar(coordenador, varredura(COLETOR_A, 1, -60))
            await aguardar(lambda: donos(broker) == [COLETOR_A])
            await entregar(coordenador, liberacao(COLETOR_A, 2))  # a primeira conexão falhou
            await aguardar(lambda: donos(broker) == [COLETOR_A, None])
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_handoff_devolve_a_posse_a_origem_se_o_vencedor_nao_conectar():
    async def cenario():
        coordenador, broker = montar_disputa()
        coordenador.margem = 5
        try:
            await entregar(
                coordenador, presenca(COLETOR_A, "online", 0), presenca(COLETOR_B, "online", 0), posse(COLETOR_A),
                estado_rssi(COLETOR_A, 1, rssi=-85),  # conectado: a origem é a favorita
            )
            coordenador._leituras_fracas[PULSEIRA] = coordenador.minimo_fracas
            with capturar_log() as log:
                await coordenador._iniciar_handoff(PULSEIRA, COLETOR_A)
                # Leitura de antes da liberação não entra na disputa (RF06)
                await entregar(coordenador, varredura(COLETOR_A, 2, -40))
                await entregar(coordenador, liberacao(COLETOR_A, 3), varredura(COLETOR_B, 3, -60))
                await aguardar(lambda: donos(broker)[-1:] == [COLETOR_A] and len(donos(broker)) == 3)
                await entregar(coordenador, estado_rssi(COLETOR_A, 4, rssi=-70))  # a origem reconecta
                await aguardar(lambda: PULSEIRA not in coordenador._disputas)

            assert donos(broker) == [None, COLETOR_B, COLETOR_A]
            assert any("Posse devolvida à origem" in r.getMessage() for r in log), log
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_disputa_sem_sinal_se_estende_e_termina_sem_vencedor():
    async def cenario():
        coordenador, broker = montar_disputa(janela=0.02)
        try:
            await entregar(coordenador, posse(None), presenca(COLETOR_A, "online", 0))
            with capturar_log() as log:
                inicio = time.monotonic()
                await coordenador._encerrar_disputa(PULSEIRA, inicio)
            assert time.monotonic() - inicio >= coordenador.janela_disputa * coordenador.fator_extensao
            assert not broker.posses()
            assert any("Disputa sem vencedor" in r.getMessage() for r in log), log
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_primeiro_sinal_na_extensao_ganha_uma_janela_completa():
    async def cenario():
        coordenador, broker = montar_disputa(janela=0.05)
        try:
            await entregar(coordenador, posse(None), presenca(COLETOR_A, "online", 0))
            tarefa = asyncio.create_task(coordenador._encerrar_disputa(PULSEIRA, time.monotonic()))
            await asyncio.sleep(0.07)  # passou da janela sem sinal: está estendendo
            await entregar(coordenador, varredura(COLETOR_A, 1, -60))
            chegada = time.monotonic()
            await aguardar(lambda: donos(broker) == [COLETOR_A])
            assert time.monotonic() - chegada >= coordenador.janela_disputa
            tarefa.cancel()
            await asyncio.gather(tarefa, return_exceptions=True)
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


def test_liberacao_controlada_libera_a_posse_com_log():
    async def cenario():
        coordenador, broker = montar()
        try:
            await entregar(coordenador, presenca(COLETOR_A, "online", 0), posse(COLETOR_A))
            with capturar_log() as log:
                await entregar(coordenador, liberacao(COLETOR_B, 1))  # não é o dono
                assert not broker.posses()
                await entregar(coordenador, liberacao(COLETOR_A, 2))
            assert donos(broker) == [None]
            assert any("Posse liberada" in r.getMessage() and COLETOR_A in r.getMessage() for r in log), log
        finally:
            await desmontar(coordenador)

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
