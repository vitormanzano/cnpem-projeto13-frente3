"""Fonte simulada: permite ver e testar o dashboard sem a pulseira."""
import asyncio
import math
import random
import time

from hub import Hub

_BASE = "0000{}-0000-1000-8000-00805f9b34fb"
SERVICOS_FALSOS = [
    {"uuid": _BASE.format("180d"), "name": "Heart Rate", "in_use": True, "chars": [
        {"uuid": _BASE.format("2a37"), "name": "Heart Rate Measurement", "props": ["notify"]},
        {"uuid": _BASE.format("2a38"), "name": "Body Sensor Location", "props": ["read"]}]},
    {"uuid": _BASE.format("180f"), "name": "Battery Service", "in_use": False, "chars": [
        {"uuid": _BASE.format("2a19"), "name": "Battery Level", "props": ["read", "notify"]}]},
    {"uuid": _BASE.format("180a"), "name": "Device Information", "in_use": False, "chars": [
        {"uuid": _BASE.format("2a29"), "name": "Manufacturer Name String", "props": ["read"]},
        {"uuid": _BASE.format("2a24"), "name": "Model Number String", "props": ["read"]},
        {"uuid": _BASE.format("2a26"), "name": "Firmware Revision String", "props": ["read"]}]},
    {"uuid": "fb005c80-02e7-f387-1cad-8acd2d8df0c8",
     "name": "Polar PMD (PPG, acelerômetro, giroscópio, magnetômetro)", "in_use": False, "chars": [
        {"uuid": "fb005c81-02e7-f387-1cad-8acd2d8df0c8", "name": "PMD: controle", "props": ["read", "write", "indicate"]},
        {"uuid": "fb005c82-02e7-f387-1cad-8acd2d8df0c8", "name": "PMD: dados", "props": ["notify"]}]},
]


async def run(hub: Hub, seed: int | None = None) -> None:
    rnd = random.Random(seed)
    hub.source = "simulado"
    hub.set_status("conectado", "", device="Simulador · sem pulseira")
    hub.set_services(SERVICOS_FALSOS)

    t0 = time.time()
    hr = 74.0
    while True:
        t = time.time() - t0

        # a cada 100 s, 6 s de "perda de conexão" para mostrar como o painel reage
        if 60 < (t % 100) < 66:
            if hub.state != "reconectando":
                hub.clear_signal()
                hub.set_status("reconectando", "Conexão perdida (simulada). Tentando reconectar…")
            await asyncio.sleep(1.0)
            continue
        if hub.state != "conectado":
            hub.set_status("conectado", "")

        alvo = 74 + 10 * math.sin(t / 25)
        hr += (alvo - hr) * 0.15 + rnd.gauss(0, 0.8)
        rssi = -68 + 14 * math.sin(t / 18) + rnd.gauss(0, 2)   # varre de ~-54 a ~-82 dBm
        hub.update_hr(round(hr))
        hub.update_rssi(round(rssi))
        await asyncio.sleep(1.0)
