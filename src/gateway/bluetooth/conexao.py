import asyncio
import logging
import os
import re
import time
from collections import deque
from collections.abc import Callable

from bleak import BleakClient, BleakScanner
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakCharacteristicNotFoundError, BleakDeviceNotFoundError, BleakError

from bluetooth.domain.erros import (
    BluetoothIndisponivel, FalhaConexao, LeituraInvalida, PlataformaSemRSSI, RSSIIndisponivel, ServicoIndisponivel,
)
from bluetooth.domain.models import EstadoConexao, LeituraFrequenciaCardiaca, LeituraRSSI, Servico
from bluetooth.domain.frequencia_cardiaca import interpretar_medicao


PADRAO_RSSI = re.compile(r"RSSI (-?\d+)")
# HCI_RSSI_INVALID: o controlador não tem o valor. Publicado, seria o "melhor sinal" da disputa
RSSI_INVALIDO_HCI = 127
# Status 0x14 do mgmt do kernel: o processo não tem CAP_NET_ADMIN
TRECHO_SEM_PERMISSAO_BTMGMT = "Permission Denied"

INTERVALO_RSSI_PADRAO = 1.0
FALHAS_MAXIMAS_RSSI_PADRAO = 3
TIMEOUT_BTMGMT = 2.0

TEMPO_SEM_LEITURA_FC_PADRAO = 5.0

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


def ler_tempo_sem_leitura_fc(tempo: float | None = None) -> float:
    """Resolve o tempo sem FC que gera o aviso em log (RF03).

    Args:
        tempo (float | None): Segundos. Se omitido, é lido da variável de ambiente
            FC_TEMPO_SEM_LEITURA (padrão de 5 s).

    Raises:
        ValueError: Se o tempo não for positivo ou a variável de ambiente não for um número.
    """
    if tempo is None:
        tempo = float(os.getenv("FC_TEMPO_SEM_LEITURA", TEMPO_SEM_LEITURA_FC_PADRAO))
    if tempo <= 0:
        raise ValueError("FC_TEMPO_SEM_LEITURA deve ser maior que zero (em segundos).")
    return tempo


def ler_intervalo_rssi(intervalo: float | None = None) -> float:
    """Resolve o intervalo entre leituras de RSSI da conexão ativa (RF04).

    Args:
        intervalo (float | None): Segundos. Se omitido, é lido da variável de ambiente
            RSSI_INTERVALO (padrão de 1 s).

    Raises:
        ValueError: Se o intervalo não for positivo ou a variável de ambiente não for um número.
    """
    if intervalo is None:
        intervalo = float(os.getenv("RSSI_INTERVALO", INTERVALO_RSSI_PADRAO))
    if intervalo <= 0:
        raise ValueError("RSSI_INTERVALO deve ser maior que zero (em segundos).")
    return intervalo


def ler_falhas_maximas_rssi(falhas: int | None = None) -> int:
    """Resolve quantas falhas seguidas de RSSI derrubam a conexão para reconectar (RF04, RF05).

    Args:
        falhas (int | None): Se omitido, é lido da variável de ambiente RSSI_MAX_FALHAS
            (padrão de 3).

    Raises:
        ValueError: Se for menor que 1 ou a variável de ambiente não for um número inteiro.
    """
    if falhas is None:
        falhas = int(os.getenv("RSSI_MAX_FALHAS", FALHAS_MAXIMAS_RSSI_PADRAO))
    if falhas < 1:
        raise ValueError("RSSI_MAX_FALHAS deve ser pelo menos 1.")
    return falhas


def interpretar_conn_info(codigo_saida: int | None, saida: str, erro: str) -> int:
    """Extrai o RSSI da resposta do `btmgmt conn-info` (RF04).

    Args:
        codigo_saida (int | None): Código de saída do btmgmt.
        saida (str): Saída padrão do btmgmt.
        erro (str): Saída de erro do btmgmt.

    Returns:
        int: RSSI da conexão ativa, em dBm.

    Raises:
        PlataformaSemRSSI: Se o processo não tiver permissão para consultar o adaptador.
        RSSIIndisponivel: Se a resposta não trouxer um RSSI válido (por exemplo, "Not Connected").
    """
    motivo = erro.strip() or saida.strip() or f"btmgmt saiu com código {codigo_saida} sem resposta"
    if TRECHO_SEM_PERMISSAO_BTMGMT in saida + erro:
        raise PlataformaSemRSSI(f"btmgmt sem permissão, rode como root ou com CAP_NET_ADMIN: {motivo}")

    correspondencia = PADRAO_RSSI.search(saida)
    if codigo_saida != 0 or correspondencia is None:
        raise RSSIIndisponivel(motivo)

    rssi = int(correspondencia.group(1))
    if rssi == RSSI_INVALIDO_HCI:
        raise RSSIIndisponivel(f"controlador informou RSSI inválido ({RSSI_INVALIDO_HCI})")
    return rssi


class ConexaoPulseira:
    """Conexão GATT com a pulseira, mantida enquanto o coletor for o responsável por ela."""

    def __init__(self, endereco: str, timeout_conexao: float | None = None,
                 ao_atualizar: TratadorLeitura | None = None,
                 tempo_sem_leitura_fc: float | None = None,
                 intervalo_rssi: float | None = None,
                 falhas_maximas_rssi: int | None = None) -> None:
        """
        Args:
            endereco (str): Endereço MAC (Linux) ou UUID (macOS) da pulseira.
            timeout_conexao (float | None): Tempo limite da conexão em segundos. Se omitido,
                é lido da variável de ambiente CONNECT_TIMEOUT (padrão de 20 s).
            ao_atualizar (TratadorLeitura | None): Chamado a cada mudança de estado e a cada RSSI lido.
            tempo_sem_leitura_fc (float | None): Segundos sem frequência cardíaca, com a conexão ativa,
                até registar o aviso em log. Se omitido, é lido da variável de ambiente
                FC_TEMPO_SEM_LEITURA (padrão de 5 s)
            intervalo_rssi (float | None): Segundos entre leituras de RSSI (RF04). Se omitido,
                é lido da variável de ambiente RSSI_INTERVALO (padrão de 1 s).
            falhas_maximas_rssi (int | None): Falhas seguidas de RSSI que derrubam a conexão para
                reconectar (RF04, RF05). Se omitido, é lido da variável de ambiente
                RSSI_MAX_FALHAS (padrão de 3).

        Raises:
            ValueError: Se alguma configuração for inválida.
        """

        self.endereco = endereco
        self.timeout_conexao = ler_timeout_conexao(timeout_conexao)
        self.tempo_sem_leitura_fc = ler_tempo_sem_leitura_fc(tempo_sem_leitura_fc)
        self.intervalo_rssi = ler_intervalo_rssi(intervalo_rssi)
        self.falhas_maximas_rssi = ler_falhas_maximas_rssi(falhas_maximas_rssi)
        self.ao_ler: TratadorLeitura | None = None
        self._estado = EstadoConexao.DESCONECTADO
        self._client: BleakClient | None = None
        self._desconectado = asyncio.Event()
        self._encerramento_solicitado = False
        self._motivo_desconexao: str | None = None
        self._leitura_fc_recebida = asyncio.Event()
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
        """Indica se a última desconexão foi uma liberação pedida pelo coletor, que não deve reconectar."""
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
            descricao=DESCRICOES_SERVICOS_POLAR.get(service.uuid.lower(), service.description),
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
        self._motivo_desconexao = None
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

    async def desconectar(self, reconectar: bool = False) -> None:
        """Encerra a conexão de forma controlada.

        Args:
            reconectar (bool): False libera a pulseira para outro coletor (RF03, RF10), e o
                manter_conexao não reconecta. True só derruba a conexão, para o manter_conexao
                tratar como queda e reconectar, como após falhas seguidas de RSSI (RF04).

        Raises:
            FalhaConexao: Se a pulseira continuar conectada depois da desconexão.
        """
        if self._client is None:
            return

        self._encerramento_solicitado = not reconectar
        # Definido antes do disconnect(): o callback do bleak registra a queda durante a chamada
        self._motivo_desconexao = "derrubada para reconectar" if reconectar else "Liberada"
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
        self._mudar_estado(EstadoConexao.DESCONECTADO, self._motivo_desconexao or "desconectou inesperadamente")
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
            ServicoIndisponivel: Se a pulseira não tiver a característica ou recusar as notificações.
        """
        try:
            await self.client.start_notify(
                CARACTERISTICA_FREQUENCIA_CARDIACA_UUID, self._ao_receber_frequencia_cardiaca
            )
        except BleakCharacteristicNotFoundError as e:
            raise ServicoIndisponivel(
                "característica Heart Rate Measurement (0x2A37) não encontrada na pulseira"
            ) from e
        except BleakError as e:
            if not self.conectado:
                raise FalhaConexao(f"Pulseira {self.endereco} caiu ao assinar a frequência cardíaca") from e
            raise ServicoIndisponivel(f"notificações da 0x2A37 recusadas: {e}") from e



    async def parar_leitura_frequencia_cardiaca(self) -> None:
        """Cancela a assinatura das notificações de frequência cardíaca."""
        await self.client.stop_notify(CARACTERISTICA_FREQUENCIA_CARDIACA_UUID)


    async def manter_vigilancia_frequencia_cardiaca(self) -> None:
        """Registra em log quando a frequência cardíaca para de chegar com a conexão ativa.

        O aviso sai uma vez por silência, e outra quando as leituras volta, para não repetir a cada
        `tempo_sem_leitura_fc`. Leituras descartadas não contam como recebidas.
        """
        em_silencio = False
        while self.conectado:
            self._leitura_fc_recebida.clear()
            try:
                await asyncio.wait_for(self._leitura_fc_recebida.wait(), self.tempo_sem_leitura_fc)
            except TimeoutError:
                # A queda também esgota o prazo; quem registra a queda é o _marcar_desconectado
                if self.conectado and not em_silencio:
                    em_silencio = True
                    logging.warning(
                        f"Estado: {self._estado.value} | Pulseira: {self.endereco} | "
                        f"Nenhuma leitura de frequência cardíaca há {self.tempo_sem_leitura_fc:g}s"
                    )
                continue
            if em_silencio:
                em_silencio = False
                logging.info(f"Pulseira: {self.endereco} | Leituras de frequência cardíaca retomadas")


    def _ao_receber_frequencia_cardiaca(self, _caracteristica: BleakGATTCharacteristic, dados: bytearray) -> None:
        """Callback síncrono do bleak: só interpreta, guarda e sinaliza"""
        try:
            leitura = interpretar_medicao(dados)
        except LeituraInvalida as e:
            logging.warning(
                f"Erro: {e.codigo} | Pulseira: {self.endereco} | "
                f"Leitura de frequência cardíaca descartada: {e} | Dados: {dados.hex()}"
            )
            return

        self._leituras_frequencia_cardiaca.append(leitura)
        self._leitura_fc_recebida.set()
        logging.debug(
            f"Pulseira: {self.endereco} | FC {leitura.bpm} | RR {list(leitura.intervalos_rr_ms)} ms"
        )



    async def ler_rssi(self) -> LeituraRSSI:
        """Lê o RSSI da conexão ativa via btmgmt (BlueZ) e o registra com o horário (RF04).

        O btmgmt consulta o controlador pelo canal de gerenciamento do kernel, fora do GATT,
        por isso a leitura não interrompe a conexão nem as notificações de frequência cardíaca.

        Returns:
            LeituraRSSI: RSSI em dBm e horário da leitura.

        Raises:
            FalhaConexao: Se a pulseira não estiver conectada.
            PlataformaSemRSSI: Se a plataforma não permitir a leitura (sem btmgmt ou sem permissão).
            RSSIIndisponivel: Se a leitura falhar (tempo esgotado ou resposta sem RSSI válido).
        """
        if not self.conectado:
            raise FalhaConexao(f"Pulseira {self.endereco} não está conectada")

        try:
            processo = await asyncio.create_subprocess_exec(
                "btmgmt", "--index", "0", "conn-info", "-t", "1", self.endereco,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (FileNotFoundError, PermissionError) as e:
            raise PlataformaSemRSSI(
                f"btmgmt indisponível ({type(e).__name__}); o RSSI da conexão só é lido no Linux com BlueZ"
            ) from e

        try:
            saida, erro = await asyncio.wait_for(processo.communicate(), TIMEOUT_BTMGMT)
        except TimeoutError as e:
            raise RSSIIndisponivel(f"btmgmt não respondeu em {TIMEOUT_BTMGMT:g}s") from e
        finally:
            # Tempo esgotado ou monitor cancelado no meio da leitura: não deixar o btmgmt órfão
            if processo.returncode is None:
                processo.kill()

        rssi = interpretar_conn_info(
            processo.returncode, saida.decode(errors="replace"), erro.decode(errors="replace")
        )
        leitura = LeituraRSSI(self.endereco, EstadoConexao.CONECTADO, rssi)
        self._leituras_rssi.append(leitura)
        self._notificar(self.ao_atualizar, leitura)
        return leitura


    async def monitorar_rssi(self) -> None:
        """Lê o RSSI a cada `intervalo_rssi` segundos enquanto a pulseira estiver conectada (RF04).

        Uma falha isolada é registrada com o motivo e a leitura segue no próximo intervalo. Se a
        plataforma não permitir a leitura, informa o motivo e encerra o monitor sem mexer na
        conexão, porque reconectar não resolveria.

        Raises:
            RSSIIndisponivel: Após `falhas_maximas_rssi` falhas seguidas, para o manter_conexao
                derrubar a conexão e reconectar (RF05).
        """
        falhas = 0
        while self.conectado:
            inicio = time.monotonic()
            try:
                leitura = await self.ler_rssi()
            except PlataformaSemRSSI as e:
                logging.error(
                    f"Erro: {e.codigo} | Pulseira: {self.endereco} | "
                    f"Monitor de RSSI encerrado, a conexão segue sem RSSI: {e}"
                )
                return
            except RSSIIndisponivel as e:
                falhas += 1
                logging.warning(
                    f"Erro: {e.codigo} | Pulseira: {self.endereco} | "
                    f"Falha seguida {falhas}/{self.falhas_maximas_rssi}: {e}"
                )
                if falhas >= self.falhas_maximas_rssi:
                    raise RSSIIndisponivel(f"{falhas} falha(s) seguida(s) na leitura de RSSI; última: {e}") from e
            else:
                falhas = 0
                self._notificar(self.ao_ler, leitura)
                logging.debug(
                    f"Estado: {leitura.estado.value} | Pulseira: {self.endereco} | "
                    f"RSSI: {leitura.rssi} dBm | Horário: {leitura.timestamp:%H:%M:%S.%f}"
                )
            # Desconta a duração do btmgmt para o intervalo valer entre o início das leituras
            await asyncio.sleep(max(0.0, self.intervalo_rssi - (time.monotonic() - inicio)))


    def _mudar_estado(self, novo: EstadoConexao, motivo: str = "") -> None:
        if novo is self._estado:
            return 

        anterior = self._estado
        self._estado = novo
        logging.info(
                f"Estado: {anterior.value} -> {novo.value} | Pulseira: {self.endereco}"
                + (f" | {motivo}" if motivo else "")
        )

        self._notificar(self.ao_atualizar, LeituraRSSI(self.endereco, novo))


    def _notificar(self, tratador: TratadorLeitura | None, leitura: LeituraRSSI) -> None:
        if tratador is None:
            return
        try:
            tratador(leitura)
        except Exception as e:
            # Uma falha na publicação não pode derrubar a conexão BLE nem o monitor de RSSI
            logging.exception(f"Erro ao notificar leitura da pulseira {self.endereco} [{type(e).__name__}]: {e}")

