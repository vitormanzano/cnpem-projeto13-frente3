import asyncio
import logging
import os
import re
import time
from collections import deque
from collections.abc import Callable

from bleak import BleakClient, BleakScanner
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakDeviceNotFoundError, BleakError

from bluetooth.domain.erros import BluetoothIndisponivel, FalhaConexao, RSSIIndisponivel
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI, Servico


PADRAO_RSSI = re.compile(r"RSSI (-?\d+)")

INTERVALO_RSSI_PADRAO = 1.0
TIMEOUT_BTMGMT = 2.0

CONNECT_TIMEOUT_PADRAO = 20.0
# O bleak 1.1.1 reporta adaptador ausente ou desligado como BleakError genérico
TRECHOS_ADAPTADOR_INDISPONIVEL = ("Bluetooth adapters found", "org.bluez.Error.NotReady")

CARACTERISTICA_FREQUENCIA_CARDIACA_UUID = "00002a37-0000-1000-8000-00805f9b34fb" # Heart Rate Measurement (0x2A37)
# Serviços proprietários da Polar sairiam como "Unknown"
DESCRICOES_SERVICOS_POLAR = {
    "fb005c80-02e7-f387-1cad-8acd2d8df0c8": "Polar Measurement Data (PMD)",
}

TAMANHO_MAXIMO_DE_LEITURAS_BUFFER = 10

TratadorLeitura = Callable[[LeituraRSSI], None]


def ler_timeout_conexao(timeout: float | None = None) -> float:
    """Resolve o tempo limite total da conexão GATT, localização incluída (RF03).

    Args:
        timeout (float | None): Segundos. Se omitido, é lido da variável de ambiente
            CONNECT_TIMEOUT (padrão de 20 s).

    Raises:
        ValueError: Se o tempo não for positivo ou a variável de ambiente não for um número.
    """
    if timeout is None:
        timeout = float(os.getenv("CONNECT_TIMEOUT", CONNECT_TIMEOUT_PADRAO))
    if timeout <= 0:
        raise ValueError("CONNECT_TIMEOUT deve ser maior que zero (em segundos).")
    return timeout


class ConexaoPulseira:
    """Conexão GATT com a pulseira, mantida enquanto o coletor for o responsável por ela."""

    def __init__(self, endereco: str, timeout_conexao: float | None = None, 
                 ao_atualizar: TratadorLeitura | None = None) -> None:
        """
        Args:
            endereco (str): Endereço MAC (Linux) ou UUID (macOS) da pulseira.
            timeout_conexao (float | None): Tempo limite da conexão em segundos. Se omitido,
                é lido da variável de ambiente CONNECT_TIMEOUT (padrão de 20 s).
        """

        self.endereco = endereco
        self.timeout_conexao = ler_timeout_conexao(timeout_conexao)
        self.ao_ler: Callable[[LeituraRSSI], None] | None = None
        self._estado = EstadoConexao.DESCONECTADO
        self._client: BleakClient | None = None
        self._desconectado = asyncio.Event()
        self._encerramento_solicitado = False
        self._leituras_frequencia_cardiaca: deque[LeituraFrequenciaCardiaca] = deque(
            maxlen = TAMANHO_MAXIMO_DE_LEITURAS_BUFFER
        )
        self._leituras_rssi: deque[LeituraRSSI]  = deque(
            maxlen = TAMANHO_MAXIMO_DE_LEITURAS_BUFFER
        )
        self.ao_atualizar = ao_atualizar


    @property
    def rssi_atual(self) -> LeituraRSSI | None:
        """Leitura de RSSI mais recente, ou None se ainda não tiver."""
        return self._leituras_rssi[-1] if self._leituras_rssi else None


    @property
    def registros_rssi(self) -> list[LeituraRSSI]:
        """Retorna os registros de RSSI do mais antigo ao mais recente"""
        return list(self._leituras_rssi)

    @property 
    def registros_frequencia(self) -> list[LeituraFrequenciaCardiaca]:
        """Retorna os registros de frequencia cardiaca do mais antigo ao mais recente"""
        return list(self._leituras_frequencia_cardiaca)

    @property
    def estado(self) -> EstadoConexao:
        """Estado atual da conexão com a pulseira."""
        return self._estado

    @property
    def conectado(self) -> bool:
        return self._client is not None and self._client.is_connected


    @property
    def desconexao_intencional(self) -> bool:
        """Indica se a ultima desconexão foi solicitada pelo coletor"""
        return self._encerramento_solicitado

    @property
    def servicos(self) -> list[Servico]:
        """Serviços GATT da pulseira conectada com UUID e descrição (RF07).

        Returns:
            list[Servico]: Um item por serviço, na ordem informada pela pulseira.

        Raises:
            FalhaConexao: Se a pulseira não estiver conectada.
        """
        if not self.conectado:
            raise FalhaConexao(f"Pulseira {self.endereco} não está conectada")

        servicos = [
            Servico(
            uuid=service.uuid,
            descricao=service.description,
            )
        for service in self._client.services
        ]
    
        return servicos


    @property
    def client(self) -> BleakClient:
        """Client Bleak da conexão ativa, para leitura de RSSI e notificações.

        Raises:
            FalhaConexao: Se a pulseira não estiver conectada.
        """
        if not self.conectado:
            raise FalhaConexao(f"Pulseira {self.endereco} não está conectada")
        return self._client

    async def conectar(self) -> None:
        """Estabelece a conexão GATT com a pulseira.

        Raises:
            FalhaConexao: Se não conectar por tempo esgotado, dispositivo ausente ou ocupado.
            BluetoothIndisponivel: Se o Bluetooth estiver desligado ou inacessível.
        """
        if self.conectado:
            logging.info(f"Pulseira {self.endereco} já está conectada")
            return

        logging.info(f"Conectando à pulseira {self.endereco} por até {self.timeout_conexao:g}s")

        self._encerramento_solicitado = False
        self._desconectado.clear()

        inicio = time.monotonic()
        try:
            # Localiza antes de conectar: só com o endereço, o bleak gasta o timeout duas vezes
            # (varredura + conexão) e o total chegaria ao dobro do CONNECT_TIMEOUT
            dispositivo = await BleakScanner.find_device_by_address(self.endereco, timeout=self.timeout_conexao)
            if dispositivo is None:
                raise self._registrar_falha("dispositivo ausente", EstadoConexao.NAO_ENCONTRADO)
            restante = self.timeout_conexao - (time.monotonic() - inicio)
            self._client = BleakClient(dispositivo, timeout=restante, disconnected_callback=self._ao_desconectar)
            await self._client.connect()
        except TimeoutError as e:
            raise self._registrar_falha(f"tempo esgotado ({self.timeout_conexao:g}s)") from e 
        except BleakDeviceNotFoundError as e:
            raise self._registrar_falha("dispositivo ausente", EstadoConexao.NAO_ENCONTRADO) from e
        except BleakError as e:
            if any(trecho in str(e) for trecho in TRECHOS_ADAPTADOR_INDISPONIVEL):
                raise self._registrar_bluetooth_indisponivel(e) from e
            raise self._registrar_falha(f"dispositivo ocupado ou recusou a conexão: {e}") from e
        except OSError as e:
            raise self._registrar_bluetooth_indisponivel(e) from e

        self._mudar_estado(EstadoConexao.CONECTADO, f"{len(self._client.services.services)} serviço(s) disponível(is)")

    async def desconectar(self) -> None:
        """Encerra a conexão de forma controlada, liberando a pulseira para outro coletor."""
        if self._client is None:
            return

        self._encerramento_solicitado = True
        logging.info(f"Desconectando da pulseira {self.endereco}")
        try:
            await self._client.disconnect()
        except BleakError as e:
            logging.warning(f"Erro ao desconectar de {self.endereco} [{type(e).__name__}]: {e}")

        if self._client.is_connected:
            logging.error(
                f"Estado: {self._estado.value} | Erro: {FalhaConexao.codigo} | "
                f"Pulseira {self.endereco} continua conectada após a desconexão"
            )
            raise FalhaConexao(f"Não foi possível liberar a pulseira {self.endereco}")
  
        self._marcar_desconectado()

    async def aguardar_desconexao(self) -> None:
        """Bloqueia até a conexão ser encerrada, seja por queda ou por desconexão controlada."""
        await self._desconectado.wait()

    async def __aenter__(self) -> "ConexaoPulseira":
        await self.conectar()
        return self

    async def __aexit__(self, *_) -> None:
        await self.desconectar()

    def _ao_desconectar(self, _client: BleakClient) -> None:
        self._marcar_desconectado()

    def _marcar_desconectado(self) -> None:
        if self._estado is EstadoConexao.DESCONECTADO and self._desconectado.is_set():
            return
        self._mudar_estado(EstadoConexao.DESCONECTADO,
            "Liberada" if self._encerramento_solicitado else "desconectou inesperadamente")
        self._desconectado.set()

    def _registrar_falha(self, motivo: str, estado: EstadoConexao =  EstadoConexao.DESCONECTADO) -> FalhaConexao:
        """Registra a falha e devolve o erro, para o chamador levantar com 'from e'"""
        self._client = None
        self._mudar_estado(estado, motivo)
        self._desconectado.set()
        logging.error(f"Estado: {self._estado.value} | Erro: {FalhaConexao.codigo} | Pulseira {self.endereco}: {motivo}")
        return FalhaConexao(motivo)


    def _registrar_bluetooth_indisponivel(self, e: Exception) -> BluetoothIndisponivel:
        self._client = None
        self._desconectado.set()
        logging.error(f"Erro: {BluetoothIndisponivel.codigo} | Pulseira: {self.endereco} | {type(e).__name__}: {e}")
        return BluetoothIndisponivel(str(e))


    async def iniciar_leitura_frequencia_cardiaca(self) -> None:
        """Assina as notificações de frequência cardíaca da pulseira.

        Raises:
            FalhaConexao: se a pulseira não estiver conectada.
        """
        await self.client.start_notify(
            CARACTERISTICA_FREQUENCIA_CARDIACA_UUID, self._ao_receber_frequencia_cardiaca
        )


    async def parar_leitura_frequencia_cardiaca(self) -> None:
        """Cancela a assinatura das notificações de frequência cardíaca."""
        await self.client.stop_notify(CARACTERISTICA_FREQUENCIA_CARDIACA_UUID)


    def _ao_receber_frequencia_cardiaca(self, _caracteristica: BleakGATTCharacteristic, data: bytearray) -> None:
        flags = data[0]
        if flags & 0x01:
            bpm = int.from_bytes(data[1:3], "little")
        else:
            bpm = data[1]

        self._leituras_frequencia_cardiaca.append(LeituraFrequenciaCardiaca(bpm = bpm))


    async def ler_rssi(self) -> LeituraRSSI:
        """Lê o RSSI da conexão ativa  via btmgmt (BlueZ).

        Raises:
            FalhaConexao: Se a pulseira não estiver conectada.
            RSSIIndisponivel: Se a plataforma não permitir leitura.
        """
        if not self.conectado:
            raise FalhaConexao(f"Pulseira {self.endereco} não está conectada.")

        try: 

            processo = await asyncio.create_subprocess_exec(
                "btmgmt", "--index", "0", "conn-info", "-t", "1", self.endereco,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

        except FileNotFoundError as e:
            raise RSSIIndisponivel("btmgmt não encontrado.") from e

        try :
            saida, erro = await asyncio.wait_for(processo.communicate(), TIMEOUT_BTMGMT)
        except asyncio.TimeoutError as e:
            processo.kill()
            raise RSSIIndisponivel(f"btmgmt não respondem em {TIMEOUT_BTMGMT}s") from e

        correspondencia = PADRAO_RSSI.search(saida.decode())

        if processo.returncode != 0 or correspondencia is None:
            raise RSSIIndisponivel(erro.decode().strip() or saida.decode().strip())

        leitura = LeituraRSSI(self.endereco, EstadoConexao.CONECTADO, int(correspondencia.group(1)))
        self._leituras_rssi.append(leitura)

        self._notificar(leitura)
        return leitura


    async def monitorar_rssi(self, intervalo: float | None = None) -> None:
        """Lê o RSSI rapidamente enquanto a pulseira está conectada.

        Args: 
            intervalo (float | None): Intervalo entre leituras em segundos. Se omitido,
            é lido da variável de ambiente RSSI_INTERVALO (padrão de 1 s).
        """
        if intervalo is None:
            intervalo = float(os.getenv("RSSI_INTERVALO", INTERVALO_RSSI_PADRAO))

        while self.conectado:
            try:
                leitura = await self.ler_rssi()
                if self.ao_ler:
                    self.ao_ler(leitura)
                logging.info(
                    f"Estado: {leitura.estado.value} | Pulseira {self.endereco} |"
                    f"RSSI {leitura.rssi} dbm às {leitura.timestamp:%H:%M:%S.%f}"
                )
            except RSSIIndisponivel as e:
                logging.error(f"Erro: {RSSIIndisponivel.codigo} | pulseira {self.endereco}: {e}")
                return
            await asyncio.sleep(intervalo)


    def _mudar_estado(self, novo: EstadoConexao, motivo: str = "") -> None:
        if novo is self._estado:
            return 

        anterior = self._estado
        self._estado = novo
        logging.info(
                f"Estado: {anterior.value} -> {novo.value} | Pulseira: {self.endereco}"
                + (f" | {motivo}" if motivo else "")
        )

        self._notificar(LeituraRSSI(self.endereco, novo))


    def _notificar(self, leitura: LeituraRSSI) -> None:
        if self.ao_atualizar is None:
            return
        try:
            self.ao_atualizar(leitura)
        except Exception as e:
            """Uma falha na publicação não pode derrubar a conexão BLE"""
            logging.exception(f"Erro ao notificar leitura da pulseira {self.endereco} [{type(e).__name__}]: {e}")

