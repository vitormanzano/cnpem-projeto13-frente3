"""Testes rápidos do dashboard (sem hardware, sem dependências extras).

Rodar na raiz do projeto:  python tests/test_dashboard.py
(também funciona com pytest)
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway" / "dashboard"))

from hub import Hub  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from sensor import describe_services, parse_heart_rate, skin_contact, valid_heart_rate  # noqa: E402


def test_fc_8_bits():
    assert parse_heart_rate(bytes([0x00, 72])) == 72


def test_fc_8_bits_com_rr():
    # flags 0x10 = intervalos RR presentes; FC continua em 8 bits
    assert parse_heart_rate(bytes([0x10, 65, 0x00, 0x04])) == 65


def test_fc_16_bits():
    assert parse_heart_rate(bytes([0x01, 0x2C, 0x01])) == 300


def test_pacote_invalido():
    for ruim in (b"", b"\x00", b"\x01\x10"):
        try:
            parse_heart_rate(ruim)
        except ValueError:
            continue
        raise AssertionError(f"deveria rejeitar {ruim!r}")


def test_contato_com_a_pele():
    assert skin_contact(bytes([0x00, 72])) is None    # pulseira não informa contato
    assert skin_contact(bytes([0x06, 72])) is True    # suportado + detectado
    assert skin_contact(bytes([0x04, 72])) is False   # suportado, sem contato


def test_fc_zero_ou_implausivel_e_descartada():
    assert valid_heart_rate(bytes([0x00, 0])) is None             # 0 bpm = sem pulso detectado
    assert valid_heart_rate(bytes([0x01, 0x2C, 0x01])) is None    # 300 bpm
    assert valid_heart_rate(bytes([0x04, 72])) is None            # sem contato com a pele
    assert valid_heart_rate(bytes([0x06, 72])) == 72
    assert valid_heart_rate(bytes([0x00, 72])) == 72


def test_lista_de_servicos():
    fc = SimpleNamespace(uuid="0000180D-0000-1000-8000-00805f9b34fb", description="Heart Rate", characteristics=[
        SimpleNamespace(uuid="00002a37-0000-1000-8000-00805f9b34fb", description="Heart Rate Measurement",
                        properties=["notify"])])
    pmd = SimpleNamespace(uuid="fb005c80-02e7-f387-1cad-8acd2d8df0c8", description="Unknown", characteristics=[])
    outro = SimpleNamespace(uuid="12345678-0000-0000-0000-000000000000", description="Unknown", characteristics=[])
    lista = describe_services([fc, pmd, outro])
    assert lista[0]["in_use"] and lista[0]["chars"][0]["props"] == ["notify"]
    assert lista[1]["name"].startswith("Polar PMD") and not lista[1]["in_use"]
    assert lista[2]["name"] == "Desconhecido"


def test_rssi_suavizado_e_limpeza():
    hub = Hub()
    hub.update_rssi(-60)
    hub.update_rssi(-80)
    assert -80 < hub.rssi_avg < -60          # EMA fica entre os dois valores
    hub.update_hr(70)
    snap = hub.snapshot()
    assert snap["hr"] == 70 and snap["rssi"] == -80
    hub.clear_signal()
    snap = hub.snapshot()
    assert snap["hr"] is None and snap["rssi"] is None


def test_valor_velho_some():
    hub = Hub()
    hub.update_hr(70)
    hub.hr_ts -= 10                           # leitura de 10 s atrás
    assert hub.snapshot()["hr"] is None


def test_ticker_publica_e_guarda_historico():
    async def cenario():
        hub = Hub()
        fila = hub.subscribe()
        tarefa = asyncio.create_task(hub.run_ticker())
        hub.update_hr(80)
        snap = await asyncio.wait_for(fila.get(), timeout=3)
        tarefa.cancel()
        return hub, snap
    hub, snap = asyncio.run(cenario())
    assert snap["hr"] == 80 and snap["stats"]["max"] == 80
    assert len(hub.history) == 1


if __name__ == "__main__":
    for nome, fn in list(globals().items()):
        if nome.startswith("test_"):
            fn()
            print("ok  ", nome)
