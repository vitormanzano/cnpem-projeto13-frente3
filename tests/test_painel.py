"""Testes do painel (RF20), sem broker, sem hardware e sem navegador.

Rodar na raiz do projeto:  python tests/test_painel.py
(também funciona com pytest)
"""
import asyncio
import inspect
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.domain.erros import BrokerIndisponivel  # noqa: E402
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI  # noqa: E402
from coordenador.coordenador import LIMIAR_CONEXAO_FRACA_PADRAO, Coordenador  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402
from painel.domain.sinal import QualidadeSinal, classificar_sinal  # noqa: E402
from painel.domain.visao import JANELA_HISTORICO, TEMPO_SEM_LEITURA, RetratoPulseira, VisaoPainel  # noqa: E402
from painel.painel import Painel, ler_janela_mediana, ler_modo_simulado, ler_pulseiras_painel  # noqa: E402
from painel.servidor import PAGINA, ServidorPainel  # noqa: E402
from painel.simulador import DURACAO_CICLO, PULSEIRA_SIMULADA, PassoRoteiro, SimuladorPulseira, roteiro  # noqa: E402

PULSEIRA = "A0:9E:1A:00:00:01"
OUTRA_PULSEIRA = "A0:9E:1A:00:00:02"
COLETOR_A = "coletor-a"
COLETOR_B = "coletor-b"
T0 = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------------ apoio

def casa(filtro: str, topico: str) -> bool:
    """Filtro de tópico do MQTT, com os curingas + e #."""
    partes_filtro, partes_topico = filtro.split("/"), topico.split("/")
    for i, parte in enumerate(partes_filtro):
        if parte == "#":
            return True
        if i >= len(partes_topico) or parte not in ("+", partes_topico[i]):
            return False
    return len(partes_filtro) == len(partes_topico)


class Barramento:
    """Broker em memória: entrega cada publicação a quem assina o tópico, passando pelo
    para_json/de_json, como na rede."""

    def __init__(self) -> None:
        self.clientes: list["BrokerFalso"] = []

    async def entregar(self, topico: str, mensagem: Mensagem) -> None:
        for cliente in self.clientes:
            if cliente.ao_receber is None or not any(casa(f, topico) for f in cliente.assinaturas):
                continue
            recebida = Mensagem.de_json(mensagem.para_json(), topico)
            assert recebida is not None, f"{topico}: {mensagem}"
            resultado = cliente.ao_receber(topico, recebida)
            if inspect.isawaitable(resultado):
                await resultado


class BrokerFalso:
    """Dublê da ConexaoBroker. Com um Barramento, as publicações chegam a quem assina."""

    def __init__(self, cliente_id: str = "painel", barramento: Barramento | None = None) -> None:
        self.cliente_id = cliente_id
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.keepalive = None
        self.assinaturas: dict[str, int] = {}
        self.publicadas: list[tuple[str, Mensagem]] = []
        self._barramento = barramento
        if barramento is not None:
            barramento.clientes.append(self)

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    def adicionar_ao_desconectar(self, _tratador: Callable) -> None:
        pass

    async def assinar(self, topico: str, qos: int = 1) -> None:
        self.assinaturas[topico] = qos

    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        if not self.conectado:
            raise BrokerIndisponivel("sem broker (teste)")
        self.publicadas.append((topico, mensagem))
        if self._barramento is not None:
            await self._barramento.entregar(topico, mensagem)

    async def manter_conexao(self) -> None:
        await asyncio.sleep(3600)


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


def posse(coletor: str | None, pulseira: str = PULSEIRA) -> tuple[str, Mensagem]:
    return Topicos.estado_posse(pulseira), Mensagem(
        TipoMensagem.POSSE, "coordenador", {"pulseira": pulseira, "coletor": coletor},
    )


def estado(
    coletor: str,
    rssi: int | None = -60,
    valor: str = "conectado",
    pulseira: str = PULSEIRA,
    amostras: int | None = None,
) -> tuple[str, Mensagem]:
    conteudo: dict = {"pulseira": pulseira, "estado": valor, "rssi": rssi}
    if amostras is not None:
        conteudo["amostras"] = amostras
    return Topicos.estado_rssi(coletor, pulseira), Mensagem(TipoMensagem.ESTADO_PULSEIRA, coletor, conteudo)


def fc(bpm: object, coletor: str = COLETOR_A, timestamp: datetime | None = None, pulseira: str = PULSEIRA) -> tuple[str, Mensagem]:
    mensagem = Mensagem(
        TipoMensagem.FREQUENCIA_CARDIACA, coletor, {"pulseira": pulseira, "coletor": coletor, "bpm": bpm},
        timestamp=timestamp or datetime.now(timezone.utc),
    )
    return Topicos.frequencia_cardiaca(pulseira), mensagem


def receber(painel: Painel, topico_mensagem: tuple[str, Mensagem]) -> None:
    """Entrega como o broker entregaria: depois do para_json/de_json."""
    topico, mensagem = topico_mensagem
    recebida = Mensagem.de_json(mensagem.para_json(), topico)
    assert recebida is not None, f"{topico}: {mensagem}"
    painel.ao_receber(topico, recebida)


def retrato(painel: Painel, pulseira: str = PULSEIRA) -> RetratoPulseira:
    return next(r for r in painel.visao.retrato(time.monotonic()) if r.pulseira == pulseira)


def leitura_rssi(rssi: int | None, estado_conexao: EstadoConexao = EstadoConexao.CONECTADO) -> LeituraRSSI:
    return LeituraRSSI(PULSEIRA, estado_conexao, rssi)


def leitura_fc(bpm: int, segundos: float) -> LeituraFrequenciaCardiaca:
    return LeituraFrequenciaCardiaca(bpm=bpm, timestamp=T0 + timedelta(seconds=segundos))


def visao_com_dono(janela_mediana: int = 3) -> VisaoPainel:
    visao = VisaoPainel([PULSEIRA], limiar=LIMIAR_CONEXAO_FRACA_PADRAO, janela_mediana=janela_mediana)
    visao.registrar_posse(PULSEIRA, COLETOR_A)
    return visao


# ------------------------------------------------------------------ consumidor do broker e pulseiras configuradas

def test_assina_posse_fc_e_estado_so_das_pulseiras_configuradas():
    broker = BrokerFalso()
    painel = Painel(broker, [PULSEIRA, OUTRA_PULSEIRA])
    asyncio.run(painel.assinar_topicos())
    assert broker.assinaturas == {
        Topicos.estado_posse(PULSEIRA): 1,
        Topicos.estado_rssi("+", PULSEIRA): 0,
        Topicos.frequencia_cardiaca(PULSEIRA): 0,
        Topicos.estado_posse(OUTRA_PULSEIRA): 1,
        Topicos.estado_rssi("+", OUTRA_PULSEIRA): 0,
        Topicos.frequencia_cardiaca(OUTRA_PULSEIRA): 0,
    }
    assert broker.ao_receber == painel.ao_receber


def test_extrair_frequencia_cardiaca_so_do_proprio_topico():
    assert Topicos.extrair_frequencia_cardiaca(Topicos.frequencia_cardiaca(PULSEIRA)) == PULSEIRA
    assert Topicos.extrair_frequencia_cardiaca(Topicos.estado_posse(PULSEIRA)) is None
    assert Topicos.extrair_frequencia_cardiaca(f"pulseiras/{PULSEIRA}/frequencia-cardiaca/extra") is None


def test_pulseiras_vem_de_pulseiras_ou_do_target_address():
    with ambiente(PULSEIRAS=f"{PULSEIRA.lower()}, {OUTRA_PULSEIRA},{PULSEIRA}", TARGET_ADDRESS="A0:9E:1A:00:00:09"):
        assert ler_pulseiras_painel() == [PULSEIRA, OUTRA_PULSEIRA]
    with ambiente(PULSEIRAS="", TARGET_ADDRESS=PULSEIRA.lower()):
        assert ler_pulseiras_painel() == [PULSEIRA]
    with ambiente(PULSEIRAS="", TARGET_ADDRESS=""):
        assert ler_pulseiras_painel() == []
    with ambiente(PULSEIRAS=f"{PULSEIRA},nao-e-mac"):
        try:
            ler_pulseiras_painel()
        except ValueError as e:
            assert "nao-e-mac" in str(e)
        else:
            raise AssertionError("endereço inválido deveria falhar")


def test_configuracao_invalida_falha_na_criacao():
    for chamada in (
        lambda: ler_janela_mediana(0),
        lambda: Painel(BrokerFalso(), []),
        lambda: Painel(BrokerFalso(), [PULSEIRA], limiar=5),
    ):
        try:
            chamada()
        except ValueError:
            continue
        raise AssertionError("configuração inválida deveria falhar")
    with ambiente(PAINEL_JANELA_MEDIANA="tres"):
        try:
            ler_janela_mediana()
        except ValueError:
            pass
        else:
            raise AssertionError("PAINEL_JANELA_MEDIANA não numérica deveria falhar")
    with ambiente(PAINEL_SIMULADO="talvez"):
        try:
            ler_modo_simulado()
        except ValueError:
            pass
        else:
            raise AssertionError("PAINEL_SIMULADO desconhecido deveria falhar")
    with ambiente(PAINEL_SIMULADO="1"):
        assert ler_modo_simulado() is True
    with ambiente(PAINEL_SIMULADO=""):
        assert ler_modo_simulado() is False


def test_pulseira_nao_configurada_e_ignorada():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    receber(painel, posse(COLETOR_A, OUTRA_PULSEIRA))
    receber(painel, estado(COLETOR_A, pulseira=OUTRA_PULSEIRA))
    receber(painel, fc(70, pulseira=OUTRA_PULSEIRA))
    assert [r.pulseira for r in painel.visao.retrato(time.monotonic())] == [PULSEIRA]
    assert painel.visao.ultimo_seq == 0


# ------------------------------------------------------------------ sinal: posse, classificação e mediana

def test_rssi_e_estado_so_do_coletor_com_a_posse():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    receber(painel, estado(COLETOR_A, rssi=-60))  # Antes da posse: ignorado
    receber(painel, posse(COLETOR_A))
    receber(painel, estado(COLETOR_A, rssi=-61))
    # Varredura de outro coletor, no mesmo padrão de tópico (RF18)
    receber(painel, estado(COLETOR_B, rssi=-50, valor="desconectado", amostras=6))

    atual = retrato(painel)
    assert atual.dono == COLETOR_A
    assert atual.estado is EstadoConexao.CONECTADO
    assert atual.rssi == -61 and atual.rssi_mediana == -61


def test_limiar_do_painel_e_o_do_coordenador():
    with ambiente(LIMIAR_CONEXAO_FRACA=""):
        assert Painel(BrokerFalso(), [PULSEIRA]).visao.limiar == LIMIAR_CONEXAO_FRACA_PADRAO == -78
    with ambiente(LIMIAR_CONEXAO_FRACA="-75"):
        assert Painel(BrokerFalso(), [PULSEIRA]).visao.limiar == -75
        assert Coordenador(BrokerFalso("coordenador"), pulseiras=[PULSEIRA]).limiar == -75


def test_classificacao_do_sinal_nas_fronteiras():
    casos = {
        -50: QualidadeSinal.OTIMO, -60: QualidadeSinal.OTIMO,
        -60.5: QualidadeSinal.BOM, -61: QualidadeSinal.BOM, -70: QualidadeSinal.BOM,
        -71: QualidadeSinal.FRACO, -78: QualidadeSinal.FRACO,
        -78.5: QualidadeSinal.CRITICO, -79: QualidadeSinal.CRITICO, -95: QualidadeSinal.CRITICO,
    }
    for rssi, esperada in casos.items():
        assert classificar_sinal(rssi, -78) is esperada, rssi
    assert classificar_sinal(None, -78) is None
    # Limiar acima de -70: o crítico continua sendo o que o coordenador conta como leitura fraca
    assert classificar_sinal(-68, -65) is QualidadeSinal.CRITICO


def test_destaque_e_a_mediana_das_ultimas_leituras_e_o_bruto_fica_visivel():
    visao = visao_com_dono(janela_mediana=3)
    for agora, rssi in enumerate([-60, -90, -62]):
        visao.registrar_estado_rssi(COLETOR_A, leitura_rssi(rssi), agora)
    atual = visao.retrato(2)[0]
    assert atual.rssi == -62 and atual.rssi_mediana == -62 and atual.qualidade is QualidadeSinal.BOM

    # Janela de 3: sai o -60, a mediana de [-90, -62, -95] é -90
    visao.registrar_estado_rssi(COLETOR_A, leitura_rssi(-95), 3)
    atual = visao.retrato(3)[0]
    assert atual.rssi == -95 and atual.rssi_mediana == -90 and atual.qualidade is QualidadeSinal.CRITICO


def test_troca_de_dono_e_desconexao_zeram_a_mediana():
    visao = visao_com_dono()
    for agora in range(3):
        visao.registrar_estado_rssi(COLETOR_A, leitura_rssi(-90), agora)

    assert visao.registrar_posse(PULSEIRA, COLETOR_B)
    atual = visao.retrato(3)[0]
    assert atual.dono == COLETOR_B and atual.estado is None and atual.rssi_mediana is None

    visao.registrar_estado_rssi(COLETOR_B, leitura_rssi(-60), 4)
    assert visao.retrato(4)[0].rssi_mediana == -60  # Não mistura com o -90 do coletor anterior

    visao.registrar_estado_rssi(COLETOR_B, leitura_rssi(None, EstadoConexao.DESCONECTADO), 5)
    atual = visao.retrato(5)[0]
    assert atual.estado is EstadoConexao.DESCONECTADO and atual.rssi is None and atual.rssi_mediana is None


def test_retrato_mostra_o_estado_e_o_coletor_com_a_posse():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    pulseira = lambda: painel.evento_atualizacao(0)[0]["retrato"]["pulseiras"][PULSEIRA]  # noqa: E731
    assert pulseira()["posse_conhecida"] is False and pulseira()["dono"] is None

    receber(painel, posse(COLETOR_A))
    receber(painel, estado(COLETOR_A, rssi=-65))
    assert pulseira()["dono"] == COLETOR_A and pulseira()["estado"] == "conectado"
    assert pulseira()["qualidade"] == "bom"

    receber(painel, posse(None))
    assert pulseira()["posse_conhecida"] is True and pulseira()["dono"] is None and pulseira()["estado"] is None


# ------------------------------------------------------------------ prazos, histórico e estatísticas

def test_valor_sem_atualizacao_ha_mais_de_5s_vira_sem_leitura():
    visao = visao_com_dono()
    visao.registrar_estado_rssi(COLETOR_A, leitura_rssi(-60), 100.0)
    visao.registrar_frequencia_cardiaca(PULSEIRA, COLETOR_A, leitura_fc(72, 0), 100.0)

    no_limite = visao.retrato(100.0 + TEMPO_SEM_LEITURA)[0]
    assert no_limite.bpm == 72 and no_limite.rssi == -60

    vencido = visao.retrato(100.1 + TEMPO_SEM_LEITURA)[0]
    assert vencido.bpm is None and vencido.rssi is None and vencido.rssi_mediana is None and vencido.qualidade is None
    assert round(vencido.bpm_idade, 1) == TEMPO_SEM_LEITURA + 0.1
    # O estado é o último publicado, com a idade: "desconectado" durante a reconexão continua visível
    assert vencido.estado is EstadoConexao.CONECTADO and vencido.estado_idade > TEMPO_SEM_LEITURA


def test_fc_repetida_ou_fora_de_ordem_e_descartada():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    for bpm, segundos in ((70, 1), (71, 1), (72, 0), (73, 2)):
        receber(painel, fc(bpm, timestamp=T0 + timedelta(seconds=segundos)))
    registros = painel.visao.historico()[PULSEIRA]
    assert [r.bpm for r in registros] == [70, 73]
    assert retrato(painel).estatisticas.quantidade == 2


def test_historico_do_painel_entrega_so_os_novos_e_guarda_15_min():
    visao = visao_com_dono()
    visao.registrar_frequencia_cardiaca(PULSEIRA, COLETOR_A, leitura_fc(70, 0), 0.0)
    visao.registrar_estado_rssi(COLETOR_A, leitura_rssi(-60), 1.0)
    visto = visao.ultimo_seq
    visao.registrar_frequencia_cardiaca(PULSEIRA, COLETOR_A, leitura_fc(71, 2), 2.0)

    novos = visao.registros_desde(visto)[PULSEIRA]
    assert [r.bpm for r in novos] == [71]
    assert [r.como_dict()["bpm"] for r in novos] == [71]
    assert novos[0].como_dict()["t"] == (T0 + timedelta(seconds=2)).timestamp()

    visao.podar(1.5 + JANELA_HISTORICO)
    assert [r.bpm for r in visao.historico()[PULSEIRA]] == [71]


def test_evento_inicial_traz_o_historico_e_a_atualizacao_so_os_novos():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    receber(painel, posse(COLETOR_A))
    receber(painel, fc(70, timestamp=T0))
    inicial, seq = painel.evento_inicial()
    assert inicial["config"]["limiar"] == LIMIAR_CONEXAO_FRACA_PADRAO
    assert inicial["config"]["pulseiras"] == [PULSEIRA]
    assert [r["bpm"] for r in inicial["historico"][PULSEIRA]] == [70]

    receber(painel, fc(75, timestamp=T0 + timedelta(seconds=1)))
    atualizacao, novo_seq = painel.evento_atualizacao(seq)
    assert [r["bpm"] for r in atualizacao["novos"][PULSEIRA]] == [75]
    assert atualizacao["retrato"]["pulseiras"][PULSEIRA]["bpm"] == 75
    assert painel.evento_atualizacao(novo_seq)[0]["novos"][PULSEIRA] == []


def test_estatisticas_minima_media_maxima_e_zerar():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    for segundos, bpm in enumerate((60, 80, 70)):
        receber(painel, fc(bpm, timestamp=T0 + timedelta(seconds=segundos)))
    estatisticas = retrato(painel).estatisticas
    assert (estatisticas.minimo, estatisticas.media, estatisticas.maximo, estatisticas.quantidade) == (60, 70, 80, 3)

    assert painel.zerar_estatisticas(PULSEIRA)
    assert not painel.zerar_estatisticas(OUTRA_PULSEIRA)
    estatisticas = retrato(painel).estatisticas
    assert estatisticas.quantidade == 0 and estatisticas.minimo is None and estatisticas.media is None

    receber(painel, fc(90, timestamp=T0 + timedelta(seconds=10)))
    estatisticas = retrato(painel).estatisticas
    assert estatisticas.minimo == estatisticas.maximo == 90


def test_mensagem_com_conteudo_invalido_e_descartada_sem_excecao():
    painel = Painel(BrokerFalso(), [PULSEIRA])
    receber(painel, posse(COLETOR_A))
    invalidas = [
        fc("72"), fc(True), fc(0), fc(72.5),
        (Topicos.frequencia_cardiaca(PULSEIRA),
         Mensagem(TipoMensagem.FREQUENCIA_CARDIACA, COLETOR_A, {"pulseira": PULSEIRA, "coletor": COLETOR_B, "bpm": 70})),
        (Topicos.frequencia_cardiaca(PULSEIRA), fc(70, pulseira=OUTRA_PULSEIRA)[1]),  # Pulseira diferente da do tópico
        estado(COLETOR_A, valor="voando"), estado(COLETOR_A, rssi=-60.5),
        (Topicos.estado_rssi(COLETOR_A, PULSEIRA), estado(COLETOR_B)[1]),  # Origem diferente do coletor do tópico
        (Topicos.estado_posse(PULSEIRA),
         Mensagem(TipoMensagem.POSSE, "coordenador", {"pulseira": PULSEIRA, "coletor": 123})),
    ]
    with capturar_log() as mensagens:
        for topico, mensagem in invalidas:
            painel.ao_receber(topico, mensagem)
    assert painel.visao.ultimo_seq == 0
    assert retrato(painel).dono == COLETOR_A
    assert sum("descartad" in m for m in mensagens) == len(invalidas)


def test_queda_do_broker_e_indicada_e_a_reconexao_nao_perde_o_sinal():
    broker = BrokerFalso()
    painel = Painel(broker, [PULSEIRA])
    receber(painel, posse(COLETOR_A))
    receber(painel, estado(COLETOR_A, rssi=-60))
    assert painel.evento_atualizacao(0)[0]["retrato"]["broker"] is True

    broker.conectado = False
    assert painel.evento_atualizacao(0)[0]["retrato"]["broker"] is False

    # Na reconexão, o broker entrega de novo a posse retida: com o mesmo dono, a mediana continua
    broker.conectado = True
    receber(painel, posse(COLETOR_A))
    receber(painel, estado(COLETOR_A, rssi=-64))
    assert retrato(painel).rssi_mediana == -62


# ------------------------------------------------------------------ modo simulado

def maior_sequencia(passos: list[PassoRoteiro], condicao: Callable[[PassoRoteiro], bool]) -> int:
    maior = atual = 0
    for passo in passos:
        atual = atual + 1 if condicao(passo) else 0
        maior = max(maior, atual)
    return maior


def test_roteiro_simulado_tem_handoff_queda_e_falta_de_fc():
    passos = [roteiro(segundo) for segundo in range(2 * DURACAO_CICLO)]
    donos = [passo.dono for passo in passos]
    assert donos[0] == 0 and donos[-1] == 0 and 1 in donos and None in donos
    assert min(p.rssi for p in passos if p.rssi is not None) < LIMIAR_CONEXAO_FRACA_PADRAO
    assert maior_sequencia(passos, lambda p: p.dono is not None and not p.conectado) > TEMPO_SEM_LEITURA
    assert maior_sequencia(passos, lambda p: p.conectado and not p.com_fc) > TEMPO_SEM_LEITURA


def test_simulador_alimenta_o_painel_pelo_broker():
    async def cenario() -> tuple[Painel, list[str | None]]:
        barramento = Barramento()
        painel = Painel(BrokerFalso("painel", barramento), [PULSEIRA_SIMULADA], simulado=True)
        await painel.assinar_topicos()
        simulador = SimuladorPulseira(
            coordenador=BrokerFalso("coordenador-simulado", barramento),
            coletores=[BrokerFalso("coletor-simulado-a", barramento), BrokerFalso("coletor-simulado-b", barramento)],
            semente=1,
        )
        donos = []
        for segundo in range(DURACAO_CICLO):
            await simulador.passo(segundo)
            donos.append(retrato(painel, PULSEIRA_SIMULADA).dono)
        return painel, donos

    painel, donos = asyncio.run(cenario())
    assert donos[0] == "coletor-simulado-a" and None in donos and donos[-1] == "coletor-simulado-b"
    registros = painel.visao.historico()[PULSEIRA_SIMULADA]
    assert any(r.bpm is not None for r in registros)
    assert {r.coletor for r in registros if r.estado is EstadoConexao.CONECTADO} == {"coletor-simulado-a", "coletor-simulado-b"}
    # A queda BLE do novo dono chega ao painel; a varredura da disputa não
    assert any(r.estado is EstadoConexao.DESCONECTADO and r.coletor == "coletor-simulado-b" for r in registros)
    assert all(r.rssi is None for r in registros if r.estado is EstadoConexao.DESCONECTADO)


# ------------------------------------------------------------------ servidor e página

async def pedir(metodo: str, caminho: str, porta: int) -> tuple[int, bytes]:
    leitor, escritor = await asyncio.open_connection("127.0.0.1", porta)
    escritor.write(f"{metodo} {caminho} HTTP/1.1\r\nHost: teste\r\nContent-Length: 0\r\n\r\n".encode())
    await escritor.drain()
    resposta = await asyncio.wait_for(leitor.read(), 3)
    escritor.close()
    cabecalho, _, corpo = resposta.partition(b"\r\n\r\n")
    return int(cabecalho.split()[1]), corpo


async def ler_evento(leitor: asyncio.StreamReader) -> tuple[str | None, dict]:
    nome, dados = None, None
    while True:
        linha = (await asyncio.wait_for(leitor.readline(), 3)).decode().rstrip("\r\n")
        if not linha:
            if dados is not None:
                return nome, dados
        elif linha.startswith("event: "):
            nome = linha.removeprefix("event: ")
        elif linha.startswith("data: "):
            dados = json.loads(linha.removeprefix("data: "))


def test_servidor_entrega_pagina_eventos_e_zera_estatisticas():
    async def cenario() -> None:
        painel = Painel(BrokerFalso(), [PULSEIRA])
        receber(painel, posse(COLETOR_A))
        receber(painel, fc(72, timestamp=T0))
        servidor = ServidorPainel(painel, host="127.0.0.1", porta=0)
        tarefas = {asyncio.create_task(servidor.executar()), asyncio.create_task(painel.manter_atualizacao())}
        try:
            await aguardar(lambda: servidor.porta_em_uso is not None)
            porta = servidor.porta_em_uso

            status, corpo = await pedir("GET", "/", porta)
            assert status == 200 and corpo == PAGINA
            assert (await pedir("GET", "/nada", porta))[0] == 404
            assert (await pedir("DELETE", "/", porta))[0] == 405
            assert (await pedir("POST", "/api/zerar?pulseira=A0:9E:1A:00:00:09", porta))[0] == 404

            leitor, escritor = await asyncio.open_connection("127.0.0.1", porta)
            escritor.write(b"GET /eventos HTTP/1.1\r\nHost: teste\r\n\r\n")
            await escritor.drain()
            assert (await leitor.readline()).startswith(b"HTTP/1.1 200")
            nome, inicio = await ler_evento(leitor)
            assert nome == "inicio" and [r["bpm"] for r in inicio["historico"][PULSEIRA]] == [72]

            receber(painel, fc(75, timestamp=T0 + timedelta(seconds=1)))
            nome, atualizacao = await ler_evento(leitor)
            assert nome is None and [r["bpm"] for r in atualizacao["novos"][PULSEIRA]] == [75]

            assert (await pedir("POST", f"/api/zerar?pulseira={PULSEIRA.lower()}", porta))[0] == 204
            assert retrato(painel).estatisticas.quantidade == 0
        finally:
            # Com o fluxo de eventos ainda aberto, o encerramento não pode travar
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.wait_for(asyncio.gather(*tarefas, return_exceptions=True), 2)

    asyncio.run(cenario())


def test_pagina_nao_carrega_nada_de_fora():
    html = PAGINA.decode("utf-8")
    assert not re.search(r"https?://|<script[^>]*\ssrc=|<link[^>]*\shref=|@import|url\(", html)
    # Faixas e limiar vêm do servidor, não ficam fixos na página
    assert "config.limiar" in html and "-80" not in html and "-78" not in html


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
