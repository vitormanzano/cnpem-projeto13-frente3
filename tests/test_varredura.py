"""Testes da varredura com a posse liberada (RF18), sem hardware e sem broker.

O bleak não é usado: `varrer_pulseira` é trocada por um scanner falso, que entrega os anúncios
que o teste mandar. Nos testes de regra, o relógio da varredura também é falso, então o tempo
de ausência é conferido sem esperar de verdade. Os testes do Coletor usam o mesmo dublê do BLE
da conexão que test_desconexao.py.

Rodar na raiz do projeto:  python tests/test_varredura.py
(também funciona com pytest)
"""
import asyncio
import os
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from types import SimpleNamespace

import bleak

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

import bluetooth.scanner as modulo_scanner  # noqa: E402
from bluetooth.conexao import ConexaoPulseira  # noqa: E402
from bluetooth.domain.models import EstadoConexao, LeituraRSSI, Pulseira  # noqa: E402
from bluetooth.scanner import (  # noqa: E402
    CODIGO_RSSI_INDISPONIVEL,
    INTERVALO_RSSI_PADRAO,
    TEMPO_AUSENCIA_PADRAO,
    VarreduraPulseira,
)
from coletor.coletor import Coletor  # noqa: E402
from mqtt.domain.models import Mensagem, TipoMensagem  # noqa: E402
from mqtt.domain.topicos import Topicos  # noqa: E402
from mqtt.estado_rssi import EstadoRssiColetor  # noqa: E402
from test_desconexao import preparar_conexao  # noqa: E402

ENDERECO = "A0:9E:1A:00:00:01"
COLETOR = "coletor-a"
SEM_RSSI = CODIGO_RSSI_INDISPONIVEL


# ------------------------------------------------------------------ dublês

class RelogioFalso:
    """Ocupa o lugar do módulo `time` dentro do scanner: o teste decide quanto tempo passou."""

    def __init__(self) -> None:
        self.agora = 1000.0

    def monotonic(self) -> float:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += segundos


@contextmanager
def relogio_falso() -> Iterator[RelogioFalso]:
    relogio, original = RelogioFalso(), modulo_scanner.time
    modulo_scanner.time = relogio
    try:
        yield relogio
    finally:
        modulo_scanner.time = original


class ScannerFalso:
    """Ocupa o lugar de `varrer_pulseira`: guarda o callback de anúncio enquanto está ligado.

    `eventos` registra quando o scanner liga e desliga; os testes do Coletor acrescentam "conectar".
    """

    def __init__(self) -> None:
        self.ao_anuncio: Callable[[Pulseira], None] | None = None
        self.eventos: list[str] = []

    @property
    def ligado(self) -> bool:
        return self.ao_anuncio is not None

    def anunciar(self, *rssis: int) -> None:
        for rssi in rssis:
            self.ao_anuncio(Pulseira(ENDERECO, "Polar Sense", rssi))


@contextmanager
def scanner_falso(atraso_parada: float = 0.0) -> Iterator[ScannerFalso]:
    """Com `atraso_parada`, desligar o scanner leva esse tempo, como o stop() do BlueZ."""
    scanner, original = ScannerFalso(), modulo_scanner.varrer_pulseira

    @asynccontextmanager
    async def varrer(endereco: str, ao_anuncio: Callable[[Pulseira], None]) -> AsyncIterator[None]:
        assert endereco == ENDERECO
        scanner.ao_anuncio = ao_anuncio
        scanner.eventos.append("varredura-ligada")
        try:
            yield
        finally:
            await asyncio.sleep(atraso_parada)
            scanner.ao_anuncio = None
            scanner.eventos.append("varredura-desligada")

    modulo_scanner.varrer_pulseira = varrer
    try:
        yield scanner
    finally:
        modulo_scanner.varrer_pulseira = original


class BleakScannerFalso:
    """Dublê do BleakScanner com só o que a varredura pode usar: ligar, desligar e receber anúncios."""

    def __init__(self, detection_callback: Callable) -> None:
        self.detection_callback = detection_callback
        self.ligado = False

    async def start(self) -> None:
        self.ligado = True

    async def stop(self) -> None:
        self.ligado = False

    def anunciar(self, endereco: str, rssi: int) -> None:
        dispositivo = SimpleNamespace(address=endereco, name=None)
        self.detection_callback(dispositivo, SimpleNamespace(local_name="Polar Sense", rssi=rssi))


class BleFalso:
    """Scanners criados e tentativas de conexão durante o teste."""

    def __init__(self) -> None:
        self.scanners: list[BleakScannerFalso] = []
        self.conexoes: list[str] = []

    @property
    def scanner_ligado(self) -> bool:
        return bool(self.scanners) and self.scanners[-1].ligado


@contextmanager
def ble_so_escuta() -> Iterator[BleFalso]:
    """Troca o BleakScanner do módulo da varredura pelo dublê e registra qualquer tentativa de
    conexão: criar um BleakClient (de qualquer módulo) ou chamar ConexaoPulseira.conectar."""
    ble = BleFalso()

    def criar_scanner(detection_callback: Callable) -> BleakScannerFalso:
        scanner = BleakScannerFalso(detection_callback)
        ble.scanners.append(scanner)
        return scanner

    def cliente_proibido(_cliente: bleak.BleakClient, *_args, **_kwargs) -> None:
        ble.conexoes.append("BleakClient")
        raise AssertionError("a varredura criou um BleakClient")

    async def conectar_proibido(_conexao: ConexaoPulseira) -> None:
        ble.conexoes.append("ConexaoPulseira.conectar")
        raise AssertionError("a varredura chamou ConexaoPulseira.conectar")

    originais = (modulo_scanner.BleakScanner, bleak.BleakClient.__init__, ConexaoPulseira.conectar)
    modulo_scanner.BleakScanner = criar_scanner
    bleak.BleakClient.__init__ = cliente_proibido
    ConexaoPulseira.conectar = conectar_proibido
    try:
        yield ble
    finally:
        modulo_scanner.BleakScanner, bleak.BleakClient.__init__, ConexaoPulseira.conectar = originais


class BrokerFalso:
    """Dublê da ConexaoBroker: guarda cada publicação e confere que ela passa no de_json."""

    def __init__(self) -> None:
        self.cliente_id = COLETOR
        self.conectado = True
        self.ao_receber = None
        self.ultima_vontade = None
        self.publicadas: list[tuple[str, Mensagem]] = []

    def adicionar_ao_conectar(self, _tratador: Callable) -> None:
        pass

    def adicionar_ao_desconectar(self, _tratador: Callable) -> None:
        pass

    async def assinar(self, _topico: str, qos: int = 1) -> None:
        pass

    async def manter_conexao(self) -> None:
        await asyncio.sleep(3600)

    async def publicar(self, topico: str, mensagem: Mensagem) -> None:
        assert Mensagem.de_json(mensagem.para_json(), topico) is not None, f"{topico}: {mensagem}"
        self.publicadas.append((topico, mensagem))
        await asyncio.sleep(0)

    def estados_pulseira(self) -> list[dict]:
        return [m.conteudo for _, m in self.publicadas if m.tipo is TipoMensagem.ESTADO_PULSEIRA]


@contextmanager
def ambiente(**variaveis: str | None) -> Iterator[None]:
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


async def aguardar(condicao: Callable[[], bool], tempo: float = 2.0) -> None:
    async def esperar() -> None:
        while not condicao():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(esperar(), tempo)


# ------------------------------------------------------------------ apoio

def varredura_ligada(relogio: RelogioFalso, tempo_ausencia: float = 5.0) -> VarreduraPulseira:
    """Varredura como o executar() a deixa ao ligar o scanner: intervalo vazio e pulseira vista agora."""
    varredura = VarreduraPulseira(ENDERECO, intervalo=1.0, tempo_ausencia=tempo_ausencia)
    varredura._rssis, varredura._visto_em = [], relogio.monotonic()
    return varredura


def anunciar(varredura: VarreduraPulseira, *rssis: int) -> None:
    for rssi in rssis:
        varredura._ao_anuncio(Pulseira(ENDERECO, "Polar Sense", rssi))


def resumo(leitura: LeituraRSSI | None) -> tuple[EstadoConexao, int | None, int | None] | None:
    """Estado, RSSI e amostras: o timestamp muda a cada execução e fica de fora."""
    if leitura is None:
        return None
    assert leitura.endereco == ENDERECO
    return leitura.estado, leitura.rssi, leitura.amostras


def montar_coletor(scanner: ScannerFalso) -> tuple[Coletor, BrokerFalso, dict[str, int]]:
    """Coletor com o BLE da conexão trocado por dublês; cada conexão entra em `scanner.eventos`."""
    broker = BrokerFalso()
    coletor = Coletor(broker, ENDERECO)
    coletor.varredura.intervalo, coletor.varredura.tempo_ausencia = 0.02, 10
    chamadas = preparar_conexao(coletor.conexao)
    conectar_dubles = coletor.conexao.conectar

    async def conectar() -> None:
        scanner.eventos.append("conectar")
        await conectar_dubles()

    coletor.conexao.conectar = conectar
    return coletor, broker, chamadas


def posse(dono: str | None) -> Mensagem:
    return Mensagem(TipoMensagem.POSSE, "coordenador", {"pulseira": ENDERECO, "coletor": dono})


async def encerrar(principal: asyncio.Task) -> None:
    principal.cancel()  # o que o SIGINT/SIGTERM faz em app.principal
    await asyncio.gather(principal, return_exceptions=True)


# ------------------------------------------------------------------ mediana e amostras (critério 2)

def test_intervalo_publica_desconectado_com_a_mediana_e_as_amostras():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio)
        anunciar(varredura, -70, -60, -90, -62, -61)  # a mediana ignora o -90 isolado; a média não
        relogio.avancar(1)

        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -62, 5)


def test_mediana_com_quantidade_par_de_anuncios():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio)
        anunciar(varredura, -60, -62, -70, -64)
        relogio.avancar(1)

        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -63, 4)


def test_cada_intervalo_so_usa_os_proprios_anuncios():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio)
        anunciar(varredura, -50, -50, -50)
        relogio.avancar(1)
        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -50, 3)

        anunciar(varredura, -80)
        relogio.avancar(1)
        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -80, 1)


def test_varredura_publica_no_topico_de_estado_e_rssi_a_cada_intervalo():
    async def cenario():
        broker = BrokerFalso()
        estado_rssi = EstadoRssiColetor(broker)
        varredura = VarreduraPulseira(ENDERECO, intervalo=0.05, tempo_ausencia=10)
        leituras: list[LeituraRSSI] = []

        def ao_ler(leitura: LeituraRSSI) -> None:
            leituras.append(leitura)
            estado_rssi.registrar(leitura)

        varredura.ao_ler = ao_ler
        with scanner_falso() as scanner:
            tarefa = asyncio.create_task(varredura.executar())
            try:
                await aguardar(lambda: scanner.ligado)
                scanner.anunciar(-70, -60, -65)
                await aguardar(lambda: leituras)
                await asyncio.sleep(0.2)  # mais quatro intervalos, sem anúncio nenhum
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        assert [resumo(leitura) for leitura in leituras] == [(EstadoConexao.DESCONECTADO, -65, 3)]
        assert not scanner.ligado  # cancelar a tarefa desliga o scanner

        await estado_rssi.publicar_pendentes()
        [(topico, mensagem)] = broker.publicadas
        assert topico == Topicos.estado_rssi(COLETOR, ENDERECO)
        assert mensagem.tipo is TipoMensagem.ESTADO_PULSEIRA
        assert mensagem.conteudo == {"pulseira": ENDERECO, "estado": "desconectado", "rssi": -65, "amostras": 3}

    asyncio.run(cenario())


def test_intervalo_e_tempo_de_ausencia_configuraveis():
    with ambiente(RSSI_INTERVALO=None, VARREDURA_TEMPO_AUSENCIA=None):
        varredura = VarreduraPulseira(ENDERECO)
        assert varredura.intervalo == INTERVALO_RSSI_PADRAO == 1.0
        assert varredura.tempo_ausencia == TEMPO_AUSENCIA_PADRAO

    with ambiente(RSSI_INTERVALO="0.5", VARREDURA_TEMPO_AUSENCIA="8"):
        varredura = VarreduraPulseira(ENDERECO)
        assert (varredura.intervalo, varredura.tempo_ausencia) == (0.5, 8.0)
        varredura = VarreduraPulseira(ENDERECO, intervalo=2.0, tempo_ausencia=3.0)  # argumento vence
        assert (varredura.intervalo, varredura.tempo_ausencia) == (2.0, 3.0)


# ------------------------------------------------------------------ anúncios sem RSSI (critério 3)

def test_anuncio_sem_rssi_fica_fora_da_mediana():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio)
        anunciar(varredura, -60, SEM_RSSI, -70, SEM_RSSI)
        relogio.avancar(1)

        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -65, 2)


def test_intervalo_sem_rssi_valido_nao_publica():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio)
        anunciar(varredura, SEM_RSSI, SEM_RSSI)
        relogio.avancar(1)

        assert varredura._leitura() is None


def test_anuncio_sem_rssi_mantem_a_pulseira_ao_alcance():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio, tempo_ausencia=5)
        for _ in range(10):  # o dobro do tempo de ausência, só com anúncios sem RSSI
            anunciar(varredura, SEM_RSSI)
            relogio.avancar(1)
            assert varredura._leitura() is None, "anúncio sem RSSI deveria contar como pulseira ao alcance"


# ------------------------------------------------------------------ nao-encontrado (critério 4)

def test_nao_encontrado_depois_do_tempo_de_ausencia():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio, tempo_ausencia=5)
        relogio.avancar(4.9)
        assert varredura._leitura() is None  # ainda dentro do tempo: intervalo vazio, nada a publicar

        relogio.avancar(0.2)
        assert resumo(varredura._leitura()) == (EstadoConexao.NAO_ENCONTRADO, None, None)


def test_ausencia_conta_a_partir_do_ultimo_anuncio():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio, tempo_ausencia=5)
        relogio.avancar(3)
        anunciar(varredura, -60)
        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -60, 1)

        relogio.avancar(4.9)  # 7,9 s desde que ligou, mas só 4,9 s desde o último anúncio
        assert varredura._leitura() is None

        relogio.avancar(0.2)
        assert resumo(varredura._leitura()) == (EstadoConexao.NAO_ENCONTRADO, None, None)


def test_nao_encontrado_se_repete_ate_a_pulseira_voltar():
    with relogio_falso() as relogio:
        varredura = varredura_ligada(relogio, tempo_ausencia=5)
        relogio.avancar(6)
        assert resumo(varredura._leitura()) == (EstadoConexao.NAO_ENCONTRADO, None, None)
        relogio.avancar(1)  # RF05: publica nao-encontrado enquanto não detectar
        assert resumo(varredura._leitura()) == (EstadoConexao.NAO_ENCONTRADO, None, None)

        anunciar(varredura, -75)
        relogio.avancar(1)
        assert resumo(varredura._leitura()) == (EstadoConexao.DESCONECTADO, -75, 1)


def test_varredura_sem_anuncios_publica_nao_encontrado_so_depois_do_tempo():
    async def cenario():
        varredura = VarreduraPulseira(ENDERECO, intervalo=0.01, tempo_ausencia=0.1)
        momentos: list[tuple[float, LeituraRSSI]] = []
        varredura.ao_ler = lambda leitura: momentos.append((time.monotonic(), leitura))

        with scanner_falso() as scanner:
            inicio = time.monotonic()
            tarefa = asyncio.create_task(varredura.executar())
            try:
                await aguardar(lambda: momentos)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        assert scanner.ligado is False
        momento, leitura = momentos[0]
        assert resumo(leitura) == (EstadoConexao.NAO_ENCONTRADO, None, None)
        assert momento - inicio >= 0.1, f"nao-encontrado saiu {momento - inicio:.3f}s depois de ligar"
        assert varredura._classificacao.atual is EstadoConexao.NAO_ENCONTRADO

    asyncio.run(cenario())


# ------------------------------------------------------------------ posse recebida (critério 5)

def test_posse_recebida_encerra_a_varredura_antes_de_conectar():
    async def cenario():
        # Desligar o scanner leva tempo: conectar antes do fim do stop() também falha o teste
        with scanner_falso(atraso_parada=0.05) as scanner:
            coletor, _, chamadas = montar_coletor(scanner)
            principal = asyncio.create_task(coletor.executar())
            try:
                await aguardar(lambda: scanner.ligado)  # sem posse retida: varrendo
                await coletor.ao_receber(Topicos.estado_posse(ENDERECO), posse(COLETOR))
                await aguardar(lambda: coletor.conexao.conectado)

                assert scanner.eventos == ["varredura-ligada", "varredura-desligada", "conectar"], scanner.eventos
                assert coletor._varredura is None
                assert chamadas["conectar"] == 1
            finally:
                await encerrar(principal)

    asyncio.run(cenario())


def test_anuncios_do_intervalo_interrompido_pela_posse_nao_sao_publicados():
    async def cenario():
        with scanner_falso() as scanner:
            coletor, broker, _ = montar_coletor(scanner)
            principal = asyncio.create_task(coletor.executar())
            try:
                await aguardar(lambda: scanner.ligado)
                scanner.anunciar(-60, -61, -62)  # intervalo aberto quando a posse chega
                await coletor.ao_receber(Topicos.estado_posse(ENDERECO), posse(COLETOR))
                await aguardar(lambda: coletor.conexao.conectado)
                await asyncio.sleep(0.1)  # cinco intervalos da varredura, se ela ainda rodasse

                estados = broker.estados_pulseira()
                assert any(e["estado"] == "conectado" for e in estados), estados
                assert not any("amostras" in e for e in estados), f"leitura da varredura depois da posse: {estados}"
            finally:
                await encerrar(principal)

    asyncio.run(cenario())


# ------------------------------------------------------------------ varredura não conecta (critério 7)

def test_varredura_so_escuta_os_anuncios_e_nao_conecta():
    async def cenario():
        varredura = VarreduraPulseira(ENDERECO, intervalo=0.02, tempo_ausencia=10)
        leituras: list[LeituraRSSI] = []
        varredura.ao_ler = leituras.append

        with ble_so_escuta() as ble:
            tarefa = asyncio.create_task(varredura.executar())
            try:
                await aguardar(lambda: ble.scanner_ligado)
                for _ in range(5):
                    ble.scanners[-1].anunciar(ENDERECO, -40)  # o sinal mais forte possível
                    await asyncio.sleep(0.02)
                await aguardar(lambda: len(leituras) >= 3 or tarefa.done())
                assert ble.conexoes == [], f"a varredura tentou conectar: {ble.conexoes}"
                assert not tarefa.done(), tarefa  # nada derrubou a varredura (ex.: método que o dublê não tem)
            finally:
                tarefa.cancel()
                await asyncio.gather(tarefa, return_exceptions=True)

        assert len(ble.scanners) == 1 and not ble.scanners[0].ligado
        assert all(leitura.estado is EstadoConexao.DESCONECTADO for leitura in leituras)

    asyncio.run(cenario())


def test_coletor_com_a_posse_liberada_nao_conecta_por_melhor_que_seja_o_sinal():
    async def cenario():
        with scanner_falso() as scanner:
            coletor, broker, chamadas = montar_coletor(scanner)
            principal = asyncio.create_task(coletor.executar())
            try:
                await aguardar(lambda: scanner.ligado)
                await coletor.ao_receber(Topicos.estado_posse(ENDERECO), posse(None))  # posse liberada retida
                for _ in range(5):
                    scanner.anunciar(-40, -41, -42)
                    await asyncio.sleep(0.02)
                await aguardar(lambda: len(broker.estados_pulseira()) >= 3)

                assert chamadas["conectar"] == 0
                assert "conectar" not in scanner.eventos
                assert coletor._sessao is None and not coletor.conexao.conectado
                assert all(e["estado"] == "desconectado" and "amostras" in e for e in broker.estados_pulseira())
            finally:
                await encerrar(principal)

    asyncio.run(cenario())


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
