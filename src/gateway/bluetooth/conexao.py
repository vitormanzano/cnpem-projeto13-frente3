import asyncio
import logging
import os
from collections import deque
import subprocess
import re
from _collections_abc import Callable

from bleak import BleakClient
from bleak.backends.service import BleakGATTServiceCollection
from bleak.exc import BleakDeviceNotFoundError, BleakError
from bleak.backends.characteristic import BleakGATTCharacteristic
from bluetooth.domain.erros import BluetoothIndisponivel, FalhaConexao, RSSIIndisponivel
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI, Servico


PADRAO_RSSI = re.compile(r"RSSI (-?\d+)")

INTERVALO_RSSI_PADRAO = 1.0
TIMEOUT_BTMGMT = 2.0

CONNECT_TIMEOUT_PADRAO = 20.0

CARACTERISTICA_FREQUENCIA_CARDIACA_UUID = "uuid"
TAMANHO_MAXIMO_DE_LEITURAS_BUFFER = 10


class ConexaoPulseira:
    """Conexão GATT com a pulseira, mantida enquanto o coletor for o responsável por ela."""

    def __init__(self, endereco: str, timeout_conexao: float | None = None) -> None:
        """
        Args:
            endereco (str): Endereço MAC (Linux) ou UUID (macOS) da pulseira.
            timeout_conexao (float | None): Tempo limite da conexão em segundos. Se omitido,
                é lido da variável de ambiente CONNECT_TIMEOUT (padrão de 20 s).
        """
        if timeout_conexao is None:
            timeout_conexao = float(os.getenv("CONNECT_TIMEOUT", CONNECT_TIMEOUT_PADRAO))

        self.endereco = endereco
        self.timeout_conexao = timeout_conexao
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
        """Serviços GATT da pulseira conectada.

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
        self._client = BleakClient(
            self.endereco,
            timeout=self.timeout_conexao,
            disconnected_callback=self._ao_desconectar,
        )

        try:
            await self._client.connect()
        except asyncio.TimeoutError:
            self._registrar_falha(f"tempo esgotado ({self.timeout_conexao:g}s)")
        except BleakDeviceNotFoundError:
            self._registrar_falha("dispositivo ausente", EstadoConexao.NAO_ENCONTRADO)
        except BleakError as e:
            self._registrar_falha(f"dispositivo ocupado ou recusou a conexão: {e}")
        except OSError as e:
            self._client = None
            self._desconectado.set()
            logging.error(f"Bluetooth indisponível [{type(e).__name__}]: {e}")
            raise BluetoothIndisponivel(str(e)) from e

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

    def _registrar_falha(self, motivo: str, estado: EstadoConexao =  EstadoConexao.DESCONECTADO) -> None:
        self._client = None
        self._mudar_estado(estado, motivo)
        self._desconectado.set()
        logging.error(f"Estado: {self._estado.value} | Erro: {FalhaConexao.codigo} | Pulseira {self.endereco}: {motivo}")
        raise FalhaConexao(motivo)


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
                "btmgmt", "--index", "0", "conn-info", "-t", "le_random", self.endereco,
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
                logging.error(f"Erro: {RSSIIndisponivel.codigo} | pulseira {self.endereco}: e")
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

