"""Estado compartilhado do dashboard.

O sensor (BLE ou simulador) atualiza FC e RSSI; um "ticker" tira uma foto
(snapshot) do estado a cada segundo, guarda no histórico e envia aos navegadores
conectados. Quem consome (a página) não precisa saber nada de BLE.
"""
import asyncio
import time
from collections import deque

RSSI_EMA_ALPHA = 0.3      # suavização do RSSI (o sinal bruto é muito ruidoso)
STALE_AFTER_S = 5.0       # sem leitura nova por este tempo => valor some do painel
HISTORY_SIZE = 900        # 15 min a 1 amostra/s


class Hub:
    def __init__(self) -> None:
        self.state = "iniciando"   # iniciando | procurando | conectando | conectado | reconectando | erro
        self.detail = ""
        self.device = ""
        self.source = "ble"        # ble | simulado

        self.hr: int | None = None
        self.hr_ts: float | None = None
        self.rssi: int | None = None
        self.rssi_avg: float | None = None
        self.rssi_ts: float | None = None

        self._hr_n = 0
        self._hr_sum = 0
        self._hr_min: int | None = None
        self._hr_max: int | None = None

        self.services: list[dict] = []   # serviços GATT da última conexão (só para exibição)

        self.history: deque[dict] = deque(maxlen=HISTORY_SIZE)
        self._subs: set[asyncio.Queue] = set()

    # ---- escrita (chamada pelo sensor) -------------------------------------
    def set_status(self, state: str, detail: str = "", device: str | None = None) -> None:
        self.state = state
        self.detail = detail
        if device is not None:
            self.device = device

    def update_hr(self, bpm: int) -> None:
        self.hr = bpm
        self.hr_ts = time.time()

    def update_rssi(self, dbm: int) -> None:
        self.rssi = dbm
        self.rssi_ts = time.time()
        if self.rssi_avg is None:
            self.rssi_avg = float(dbm)
        else:
            self.rssi_avg += RSSI_EMA_ALPHA * (dbm - self.rssi_avg)

    def set_services(self, services: list[dict]) -> None:
        self.services = services

    def clear_signal(self) -> None:
        """Perdeu a conexão: some com os valores antigos em vez de congelá-los."""
        self.hr = self.hr_ts = None
        self.rssi = self.rssi_avg = self.rssi_ts = None

    # ---- leitura -----------------------------------------------------------
    def snapshot(self) -> dict:
        now = time.time()
        hr_age = None if self.hr_ts is None else now - self.hr_ts
        rssi_age = None if self.rssi_ts is None else now - self.rssi_ts
        hr_ok = hr_age is not None and hr_age <= STALE_AFTER_S
        rssi_ok = rssi_age is not None and rssi_age <= STALE_AFTER_S
        return {
            "t": round(now, 2),
            "state": self.state,
            "detail": self.detail,
            "device": self.device,
            "source": self.source,
            "hr": self.hr if hr_ok else None,
            "hr_age": None if hr_age is None else round(hr_age, 1),
            "rssi": self.rssi if rssi_ok else None,
            "rssi_avg": round(self.rssi_avg, 1) if rssi_ok and self.rssi_avg is not None else None,
            "stats": {
                "min": self._hr_min,
                "max": self._hr_max,
                "avg": round(self._hr_sum / self._hr_n) if self._hr_n else None,
            },
        }

    # ---- distribuição para os navegadores ----------------------------------
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    async def run_ticker(self) -> None:
        while True:
            snap = self.snapshot()
            if snap["hr"] is not None:
                bpm = snap["hr"]
                self._hr_n += 1
                self._hr_sum += bpm
                self._hr_min = bpm if self._hr_min is None else min(self._hr_min, bpm)
                self._hr_max = bpm if self._hr_max is None else max(self._hr_max, bpm)
                snap["stats"] = {"min": self._hr_min, "max": self._hr_max,
                                 "avg": round(self._hr_sum / self._hr_n)}
            self.history.append(snap)
            for q in list(self._subs):
                try:
                    q.put_nowait(snap)
                except asyncio.QueueFull:
                    pass  # cliente lento: perde um quadro, não trava os outros
            await asyncio.sleep(1.0)
