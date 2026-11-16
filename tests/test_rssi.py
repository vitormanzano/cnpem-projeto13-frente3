"""Testes da leitura de RSSI da conexão ativa (RF04), sem hardware.

O bleak não é usado: a conexão recebe um cliente falso só com `is_connected`, e o btmgmt é
trocado por um script no PATH.

Rodar na raiz do projeto:  python tests/test_rssi.py
(também funciona com pytest)
"""
import asyncio
import os
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

import bluetooth.conexao as modulo_conexao  # noqa: E402
from bluetooth.conexao import (  # noqa: E402
    FALHAS_MAXIMAS_RSSI_PADRAO,
    INTERVALO_RSSI_PADRAO,
    ConexaoPulseira,
    interpretar_conn_info,
    ler_falhas_maximas_rssi,
    ler_intervalo_rssi,
)
from bluetooth.domain.erros import PlataformaSemRSSI, RSSIIndisponivel  # noqa: E402
from bluetooth.domain.models import EstadoConexao, LeituraRSSI  # noqa: E402

ENDERECO = "A0:9E:1A:00:00:01"
RESPOSTA_OK = (
    f"Connection Information for {ENDERECO} (LE Public)\n"
    "\tRSSI -67\tTX power 4\tmaximum TX power 4\n"
)


class ClienteFalso:
    """Só o que a ConexaoPulseira consulta para saber se está conectada."""
    is_connected = True


def conexao_conectada(**kwargs) -> ConexaoPulseira:
    conexao = ConexaoPulseira(ENDERECO, **kwargs)
    conexao._client = ClienteFalso()
    return conexao


def roteirizar_leituras(conexao: ConexaoPulseira, roteiro: list, duracao: float = 0.0) -> list:
    """Troca o ler_rssi da conexão por um roteiro: cada item é um RSSI (int) ou um erro a levantar.

    O último item desconecta a pulseira, o que encerra o monitor. Devolve os itens já consumidos.
    """
    consumidos = []

    async def ler_rssi() -> LeituraRSSI:
        if duracao:
            await asyncio.sleep(duracao)
        item = roteiro.pop(0)
        consumidos.append(item)
        if not roteiro:
            conexao._client.is_connected = False
        if isinstance(item, Exception):
            raise item
        return LeituraRSSI(conexao.endereco, EstadoConexao.CONECTADO, item)

    conexao.ler_rssi = ler_rssi
    return consumidos


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


@contextmanager
def btmgmt_falso(script: str | None):
    """Coloca no PATH um btmgmt que roda `script` (sh). Com None, o PATH fica sem btmgmt."""
    with tempfile.TemporaryDirectory() as pasta:
        caminho = os.environ.get("PATH", "")
        if script is not None:
            arquivo = Path(pasta) / "btmgmt"
            arquivo.write_text(f"#!/bin/sh\n{script}\n")
            arquivo.chmod(arquivo.stat().st_mode | stat.S_IXUSR)
            caminho = f"{pasta}{os.pathsep}{caminho}"
        else:
            caminho = pasta
        with ambiente(PATH=caminho):
            yield Path(pasta)


def processo_vivo(pid: int) -> bool:
    """Vivo e não zumbi (Linux)."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except FileNotFoundError:
        return False
    return "State:\tZ" not in status


# ------------------------------------------------------------------ resposta do btmgmt

def test_interpreta_o_rssi_em_dbm():
    assert interpretar_conn_info(0, RESPOSTA_OK, "") == -67


def test_resposta_sem_rssi_informa_o_motivo():
    resposta = f"Could not get conn info for {ENDERECO} (LE Public): Not Connected (0x02)"
    try:
        interpretar_conn_info(1, resposta, "")
    except PlataformaSemRSSI:
        raise AssertionError("Not Connected é falha de leitura, não limite da plataforma")
    except RSSIIndisponivel as e:
        assert "Not Connected" in str(e)
        return
    raise AssertionError("deveria lançar RSSIIndisponivel")


def test_sem_permissao_e_limite_da_plataforma():
    resposta = f"Could not get conn info for {ENDERECO} (LE Public): Permission Denied (0x14)"
    try:
        interpretar_conn_info(1, resposta, "")
    except PlataformaSemRSSI as e:
        assert e.codigo == "rssi-indisponivel"
        assert "Permission Denied" in str(e)
        return
    raise AssertionError("deveria lançar PlataformaSemRSSI")


def test_rssi_invalido_do_controlador_e_descartado():
    try:
        interpretar_conn_info(0, RESPOSTA_OK.replace("-67", "127"), "")
    except RSSIIndisponivel:
        return
    raise AssertionError("127 (HCI_RSSI_INVALID) não pode virar leitura")


def test_resposta_vazia_ainda_tem_motivo():
    try:
        interpretar_conn_info(3, "", "")
    except RSSIIndisponivel as e:
        assert "3" in str(e)
        return
    raise AssertionError("deveria lançar RSSIIndisponivel")


# ------------------------------------------------------------------ ler_rssi com o btmgmt falso

def test_leitura_registra_dbm_e_horario():
    async def cenario():
        notificadas: list[LeituraRSSI] = []
        conexao = conexao_conectada(ao_atualizar=notificadas.append)
        with btmgmt_falso(f"printf '{RESPOSTA_OK}'"):
            leitura = await conexao.ler_rssi()

        assert leitura.rssi == -67
        assert leitura.estado is EstadoConexao.CONECTADO
        assert leitura.timestamp.tzinfo is timezone.utc
        assert datetime.now(timezone.utc) - leitura.timestamp < timedelta(seconds=5)
        assert conexao.rssi_atual is leitura
        assert notificadas == [leitura]

    asyncio.run(cenario())


def test_sem_btmgmt_informa_plataforma_sem_rssi():
    async def cenario():
        conexao = conexao_conectada()
        with btmgmt_falso(None):
            try:
                await conexao.ler_rssi()
            except PlataformaSemRSSI as e:
                assert "btmgmt" in str(e)
                return
        raise AssertionError("deveria lançar PlataformaSemRSSI")

    asyncio.run(cenario())


def test_btmgmt_travado_esgota_o_tempo_e_e_encerrado():
    async def cenario():
        conexao = conexao_conectada()
        original = modulo_conexao.TIMEOUT_BTMGMT
        modulo_conexao.TIMEOUT_BTMGMT = 0.2
        try:
            with btmgmt_falso('echo $$ > "$(dirname "$0")/pid"; exec sleep 5') as pasta:
                try:
                    await conexao.ler_rssi()
                    raise AssertionError("deveria lançar RSSIIndisponivel")
                except PlataformaSemRSSI:
                    raise AssertionError("tempo esgotado é falha de leitura, não limite da plataforma")
                except RSSIIndisponivel:
                    pass
                pid = int((pasta / "pid").read_text())
        finally:
            modulo_conexao.TIMEOUT_BTMGMT = original

        limite = time.monotonic() + 2
        while processo_vivo(pid) and time.monotonic() < limite:
            await asyncio.sleep(0.05)
        assert not processo_vivo(pid), "btmgmt ficou órfão"

    asyncio.run(cenario())


# ------------------------------------------------------------------ monitorar_rssi

def test_falha_isolada_nao_para_o_monitor():
    async def cenario():
        lidas: list[LeituraRSSI] = []
        conexao = conexao_conectada(intervalo_rssi=0.001, falhas_maximas_rssi=2)
        conexao.ao_ler = lidas.append
        roteirizar_leituras(conexao, [-60, RSSIIndisponivel("Not Connected"), -61, RSSIIndisponivel("x"), -62])

        await asyncio.wait_for(conexao.monitorar_rssi(), 2.0)  # termina sem erro ao desconectar
        assert [leitura.rssi for leitura in lidas] == [-60, -61, -62]

    asyncio.run(cenario())


def test_falhas_seguidas_encerram_o_monitor_com_erro():
    async def cenario():
        conexao = conexao_conectada(intervalo_rssi=0.001, falhas_maximas_rssi=3)
        falha = RSSIIndisponivel("Not Connected")
        consumidos = roteirizar_leituras(conexao, [-60, falha, falha, falha, -61])

        try:
            await asyncio.wait_for(conexao.monitorar_rssi(), 2.0)
        except RSSIIndisponivel as e:
            assert "3 falha(s) seguida(s)" in str(e)
        else:
            raise AssertionError("deveria lançar RSSIIndisponivel na 3ª falha seguida")
        assert len(consumidos) == 4  # parou na 3ª falha, sem ler o -61

    asyncio.run(cenario())


def test_plataforma_sem_rssi_encerra_o_monitor_sem_erro():
    async def cenario():
        conexao = conexao_conectada(intervalo_rssi=0.001)
        consumidos = roteirizar_leituras(conexao, [PlataformaSemRSSI("btmgmt indisponível"), -60])

        await asyncio.wait_for(conexao.monitorar_rssi(), 2.0)
        assert len(consumidos) == 1
        assert conexao.conectado  # a conexão segue sem RSSI

    asyncio.run(cenario())


def test_falha_no_tratador_nao_para_o_monitor():
    async def cenario():
        conexao = conexao_conectada(intervalo_rssi=0.001)

        def tratador_quebrado(_leitura: LeituraRSSI) -> None:
            raise RuntimeError("broker caiu")

        conexao.ao_ler = tratador_quebrado
        consumidos = roteirizar_leituras(conexao, [-60, -61, -62])

        await asyncio.wait_for(conexao.monitorar_rssi(), 2.0)
        assert consumidos == [-60, -61, -62]

    asyncio.run(cenario())


def test_intervalo_desconta_a_duracao_da_leitura():
    async def cenario():
        inicios: list[float] = []
        conexao = conexao_conectada(intervalo_rssi=0.15)
        conexao.ao_ler = lambda _leitura: inicios.append(time.monotonic())
        # Cada leitura leva 0,1 s: sem descontar, o passo seria 0,25 s
        roteirizar_leituras(conexao, [-60, -61, -62, -63], duracao=0.1)

        await asyncio.wait_for(conexao.monitorar_rssi(), 3.0)
        passos = [b - a for a, b in zip(inicios, inicios[1:])]
        media = sum(passos) / len(passos)
        assert 0.14 <= media < 0.2, f"passo médio de {media:.3f}s"

    asyncio.run(cenario())


# ------------------------------------------------------------------ configuração

def test_configuracao_por_variavel_de_ambiente():
    with ambiente(RSSI_INTERVALO=None, RSSI_MAX_FALHAS=None):
        assert ler_intervalo_rssi() == INTERVALO_RSSI_PADRAO
        assert ler_falhas_maximas_rssi() == FALHAS_MAXIMAS_RSSI_PADRAO

    with ambiente(RSSI_INTERVALO="0.5", RSSI_MAX_FALHAS="5"):
        conexao = ConexaoPulseira(ENDERECO)
        assert (conexao.intervalo_rssi, conexao.falhas_maximas_rssi) == (0.5, 5)
        conexao = ConexaoPulseira(ENDERECO, intervalo_rssi=1.0, falhas_maximas_rssi=2)  # argumento vence
        assert (conexao.intervalo_rssi, conexao.falhas_maximas_rssi) == (1.0, 2)


def test_configuracao_invalida_e_rejeitada():
    for leitor, valor in ((ler_intervalo_rssi, 0), (ler_intervalo_rssi, -1.0), (ler_falhas_maximas_rssi, 0)):
        try:
            leitor(valor)
        except ValueError:
            continue
        raise AssertionError(f"{leitor.__name__} deveria rejeitar {valor}")

    for nome, valor in (("RSSI_INTERVALO", "rápido"), ("RSSI_MAX_FALHAS", "1.5")):
        with ambiente(**{nome: valor}):
            try:
                ConexaoPulseira(ENDERECO)
            except ValueError:
                continue
        raise AssertionError(f"{nome}={valor} deveria falhar na inicialização")


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
