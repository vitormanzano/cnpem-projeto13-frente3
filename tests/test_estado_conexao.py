"""Testes da classificação do estado da conexão (RF08), sem hardware.

Rodar na raiz do projeto:  python tests/test_estado_conexao.py
(também funciona com pytest)
"""
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

from bluetooth.conexao import ConexaoPulseira  # noqa: E402
from bluetooth.domain.classificacao_conexao import ClassificacaoConexao  # noqa: E402
from bluetooth.domain.models import EstadoConexao, LeituraRSSI  # noqa: E402
from bluetooth.scanner import VarreduraPulseira  # noqa: E402

ENDERECO = "A0:9E:1A:00:00:01"


class CapturaLog(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.mensagens: list[str] = []

    def emit(self, registro: logging.LogRecord) -> None:
        self.mensagens.append(registro.getMessage())


@contextmanager
def transicoes_no_log() -> Iterator[list[str]]:
    """Guarda as linhas de mudança de estado ("anterior -> novo") emitidas dentro do bloco."""
    captura = CapturaLog()
    raiz = logging.getLogger()
    nivel = raiz.level
    raiz.addHandler(captura)
    raiz.setLevel(logging.INFO)
    transicoes: list[str] = []
    try:
        yield transicoes
    finally:
        raiz.removeHandler(captura)
        raiz.setLevel(nivel)
        transicoes.extend(m for m in captura.mensagens if " -> " in m)


def leitura_varredura(estado: EstadoConexao) -> LeituraRSSI:
    rssi = -60 if estado is EstadoConexao.DESCONECTADO else None
    return LeituraRSSI(ENDERECO, estado, rssi=rssi)


def test_classificacao_devolve_o_anterior_so_quando_muda():
    classificacao = ClassificacaoConexao()
    assert classificacao.atual is EstadoConexao.DESCONECTADO
    assert classificacao.mudar(EstadoConexao.DESCONECTADO) is None
    assert classificacao.mudar(EstadoConexao.CONECTADO) is EstadoConexao.DESCONECTADO
    assert classificacao.atual is EstadoConexao.CONECTADO


def test_varredura_registra_cada_mudanca_uma_vez():
    varredura = VarreduraPulseira(ENDERECO, intervalo=1, tempo_ausencia=5)
    with transicoes_no_log() as transicoes:
        for estado in (EstadoConexao.NAO_ENCONTRADO, EstadoConexao.NAO_ENCONTRADO,
                       EstadoConexao.DESCONECTADO, EstadoConexao.DESCONECTADO):
            varredura._registrar_mudanca(leitura_varredura(estado))

    assert len(transicoes) == 2, transicoes
    assert transicoes[0].startswith("Estado: desconectado -> nao-encontrado")
    assert transicoes[1].startswith("Estado: nao-encontrado -> desconectado")


def test_volta_da_conexao_para_a_varredura_fica_no_log():
    # Posse recebida, a pulseira sumiu antes de conectar; o coletor volta à varredura e a encontra
    classificacao = ClassificacaoConexao()
    conexao = ConexaoPulseira(ENDERECO, classificacao=classificacao)
    varredura = VarreduraPulseira(ENDERECO, classificacao=classificacao)
    with transicoes_no_log() as transicoes:
        conexao._registrar_falha("dispositivo ausente", EstadoConexao.NAO_ENCONTRADO)
        varredura._registrar_mudanca(leitura_varredura(EstadoConexao.DESCONECTADO))

    assert conexao.estado is EstadoConexao.DESCONECTADO
    assert len(transicoes) == 2, transicoes
    assert transicoes[0].startswith("Estado: desconectado -> nao-encontrado")
    assert transicoes[1].startswith("Estado: nao-encontrado -> desconectado")


def test_conexao_parte_do_estado_deixado_pela_varredura():
    classificacao = ClassificacaoConexao()
    conexao = ConexaoPulseira(ENDERECO, classificacao=classificacao)
    varredura = VarreduraPulseira(ENDERECO, classificacao=classificacao)
    with transicoes_no_log() as transicoes:
        varredura._registrar_mudanca(leitura_varredura(EstadoConexao.NAO_ENCONTRADO))
        conexao._mudar_estado(EstadoConexao.CONECTADO)

    assert transicoes[-1].startswith("Estado: nao-encontrado -> conectado"), transicoes


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
