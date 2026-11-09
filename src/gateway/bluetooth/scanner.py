import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from statistics import median

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from bleak.exc import BleakError

from bluetooth.domain.erros import BluetoothIndisponivel, PulseiraNaoEncontrada
from bluetooth.domain.models import DispositivoBLE, EstadoConexao, LeituraRSSI, Pulseira


SCAN_TIMEOUT_PADRAO          = 10.0
INTERVALO_RSSI_PADRAO        = 1.0
TEMPO_AUSENCIA_PADRAO        = 5.0
INTERVALO_RETOMADA           = 5.0

CODIGO_RSSI_INDISPONIVEL     = 127

NOME_PADRAO                  = "Desconhecido / Oculto"
MENSAGEM_BLUETOOTH_DESLIGADO = (
    "Bluetooth desligado ou indisponível. Ative o Bluetooth do coletor e tente novamente."
)

async def find_all_local_devices(timeout: float | None = None) -> list[DispositivoBLE]:
    """Lista os dispositivos BLE anunciando ao alcance do coletor (RF01).

    Cada dispositivo aparece uma única vez (o Bleak agrupa por endereço),
    com o RSSI do anúncio mais recente.

    Args:
        timeout (float): Duração da varredura em segundos.

    Returns:
        list[DispositivoBLE]: Dispositivos ordenados do sinal mais forte para o mais fraco.
            Lista vazia caso nenhum seja encontrado.

    Raises:
        BluetoothIndisponivel: Se o Bluetooth estiver desligado ou inacessível.
    """
    if timeout is None:
        timeout = float(os.getenv("SCAN_TIMEOUT", SCAN_TIMEOUT_PADRAO))

    logging.info(f"Iniciando varredura BLE por {timeout:g}s")

    try:
        devices_dict = await BleakScanner.discover(return_adv=True, timeout=timeout)
    except (BleakError, OSError) as e:
        logging.error(f"Falha ao iniciar varredura BLE [{type(e).__name__}]: {e}")
        raise BluetoothIndisponivel(str(e)) from e

    dispositivos = [
        DispositivoBLE(
            endereco=address,
            nome=adv_data.local_name or dev.name or NOME_PADRAO,
            rssi=adv_data.rssi,
        )
        for address, (dev, adv_data) in devices_dict.items()
    ]
    dispositivos.sort(key=lambda d: d.rssi, reverse=True)

    logging.info(f"Varredura concluída: {len(dispositivos)} dispositivo(s) encontrado(s)")
    return dispositivos



@asynccontextmanager
async def varrer_pulseira(endereco: str, ao_anuncio: Callable[[Pulseira], None]) -> AsyncIterator[None]:
    """Mantém o scanner ligado enquanto o bloco `async with` roda, chamando `ao_anuncio` a cada anúncio da pulseira.
    
    Raises:
        BluetoothIndisponviel: Se o scanner não puder ser ligado
    """
    def ao_detectar(dev: BLEDevice, adv: AdvertisementData) -> None:
        if dev.address.upper() == endereco.upper():
            ao_anuncio(Pulseira(endereco=dev.address, nome=adv.local_name or dev.name or NOME_PADRAO, rssi=adv.rssi))

    scanner = BleakScanner(detection_callback=ao_detectar)
    try:
        await scanner.start()
    except (BleakError, OSError) as e:
        logging.error(f"{MENSAGEM_BLUETOOTH_DESLIGADO} [{type(e).__name__}]: {e}")
        raise BluetoothIndisponivel(MENSAGEM_BLUETOOTH_DESLIGADO) from e
    try:
        yield
    finally:
        try:
            await scanner.stop()
        except (BleakError, OSError) as e:
            logging.warning(f"Falha ao parar a varredura [{type(e).__name__}]: {e}")


class VarreduraPulseira:
    """Escuta os anúncios da pulseira sem se conectar e entrega uma leitura a cada `intervalo`."""

    def __init__(self, endereco: str, intervalo: float | None = None, tempo_ausencia: float | None = None) -> None:
        self.endereco = endereco
        self.ao_ler: Callable[[LeituraRSSI], None] | None = None
        self.intervalo = intervalo or float(os.getenv("RSSI_INTERVALO", INTERVALO_RSSI_PADRAO))
        self.tempo_ausencia = tempo_ausencia or float(os.getenv("VARREDURA_TEMPO_AUSENCIA", TEMPO_AUSENCIA_PADRAO))
        self._ultimo: Pulseira | None = None
        self._rssis: list[int] = [] # RSSI de cada anúncio no intervalo atual
        self._nome: str | None = None
        self._visto_em = 0.0
        self._estado: EstadoConexao | None = None


    def _ao_anuncio(self, pulseira: Pulseira) -> None:
        """Acumula o RSSI de cada anúncio. Anúncio sem RSSI supõe que a pulseira está ao alcance, mas fica fora da mediana."""
        self._visto_em, self._nome = time.monotonic(), pulseira.nome
        if pulseira.rssi != CODIGO_RSSI_INDISPONIVEL:
            self._rssis.append(pulseira.rssi)



    def _leitura(self) -> LeituraRSSI | None:
        rssis, self._rssis = self._rssis, [] # Fecha o intervalo, e o próximo começa vazio
        if time.monotonic() - self._visto_em > self.tempo_ausencia: 
            return LeituraRSSI(self.endereco, EstadoConexao.NAO_ENCONTRADO) 
        if not rssis:
            return None # Nenhum RSSI válido nesse intervalo, não publica
        return LeituraRSSI(self.endereco, EstadoConexao.DESCONECTADO, rssi=round(median(rssis)), amostras=len(rssis))


    def _registrar_mudanca(self, leitura: LeituraRSSI) -> None:
        """Loga só quando a pulseira aparece ou some, não a cada segundo."""
        if leitura.estado is self._estado:
            return
        self._estado = leitura.estado
        if leitura.estado is EstadoConexao.NAO_ENCONTRADO:
            logging.warning(
                f"Estado: {leitura.estado.value} | Erro: {PulseiraNaoEncontrada.codigo} | "
                f"Pulseira {self.endereco} sem anúncios há {self.tempo_ausencia} s"
            )
        else:
            logging.info(f"Pulseira encontrada: {self.endereco} | {self._nome} | {leitura.rssi} dBm.")


    async def executar(self) -> None:
        """Varre até a tarefa ser cancelada; se o Bluetooth cair, tenta de novo."""
        while True:
            self._rssis, self._visto_em = [], time.monotonic()
            try:
                async with varrer_pulseira(self.endereco, self._ao_anuncio):
                    while True:
                        await asyncio.sleep(self.intervalo)
                        if leitura:= self._leitura():
                            self._registrar_mudanca(leitura)
                            if self.ao_ler:
                                self.ao_ler(leitura)
            except BluetoothIndisponivel:
                await asyncio.sleep(INTERVALO_RETOMADA)
