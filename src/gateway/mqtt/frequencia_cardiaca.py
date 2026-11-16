import asyncio
import logging
import os
from collections import deque

from bluetooth.domain.erros import BrokerIndisponivel
from bluetooth.domain.models import LeituraFrequenciaCardiaca
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos


MAX_LEITURAS_SEM_BROKER_PADRAO = 15


def ler_max_leituras_sem_broker(maximo: int | None = None) -> int:
    """Resolve quantas leituras de frequência cardíaca ficam guardadas sem broker (RF17).

    Args:
        maximo (int | None): Número de leituras. Se omitido, é lido de FC_MAX_LEITURAS_SEM_BROKER
            (padrão de 15).

    Raises:
        ValueError: Se não for um inteiro maior que zero.
    """
    if maximo is None:
        valor = os.getenv("FC_MAX_LEITURAS_SEM_BROKER") or str(MAX_LEITURAS_SEM_BROKER_PADRAO)
        try:
            maximo = int(valor)
        except ValueError as e:
            raise ValueError(f"FC_MAX_LEITURAS_SEM_BROKER deve ser um inteiro, recebido {valor!r}.") from e

    if maximo <= 0:
        raise ValueError("FC_MAX_LEITURAS_SEM_BROKER deve ser maior que zero.")
    return maximo


class FrequenciaCardiacaColetor:
    """Publica a frequência cardíaca no tópico da pulseira enquanto o coletor detém a posse (RF17).

    Só publica com a posse confirmada pela mensagem de posse do coordenador. A queda do broker
    suspende a confirmação até a posse retida chegar na reconexão: sem isso, o buffer seria
    esvaziado antes de o coletor saber se a pulseira ainda é dele.
    """

    def __init__(
        self,
        broker: ConexaoBroker,
        endereco_pulseira: str,
        max_leituras_sem_broker: int | None = None,
    ) -> None:
        """
        Args:
            broker (ConexaoBroker): Sessão MQTT do coletor.
            endereco_pulseira (str): Endereço BLE da pulseira, que forma o tópico.
            max_leituras_sem_broker (int | None): Leituras guardadas enquanto não dá para publicar;
                com o buffer cheio, a mais antiga é descartada. Se omitido, é lido de
                FC_MAX_LEITURAS_SEM_BROKER (padrão de 15).

        Raises:
            ValueError: Se `max_leituras_sem_broker` for inválido.
        """
        self.broker = broker
        self.endereco = endereco_pulseira
        self._pendentes: deque[LeituraFrequenciaCardiaca] = deque(
            maxlen=ler_max_leituras_sem_broker(max_leituras_sem_broker)
        )
        self._com_posse = False                  # Aceita leituras
        self._posse_confirmada = asyncio.Event() # Pode publicar: posse confirmada desde a última conexão ao broker
        self._nova_leitura = asyncio.Event()
        self._descartadas = 0                    # Mais antigas perdidas com o buffer cheio, para o log
        self.broker.adicionar_ao_desconectar(self.suspender)


    @property
    def pendentes(self) -> int:
        """Leituras guardadas aguardando publicação."""
        return len(self._pendentes)


    def registrar(self, leitura: LeituraFrequenciaCardiaca) -> None:
        """Guarda uma leitura para publicar. Síncrono e nunca bloqueia: roda dentro da notificação do bleak."""
        if not self._com_posse:
            # Notificação atrasada depois de perder a posse: publicada, duplicaria a do novo dono
            return
        if len(self._pendentes) == self._pendentes.maxlen:
            self._descartadas += 1 # O append no deque cheio descarta a mais antiga
            logging.debug(f"Pulseira: {self.endereco} | Buffer de frequência cardíaca cheio, descartada a mais antiga")
        self._pendentes.append(leitura)
        self._nova_leitura.set()


    def receber_posse(self) -> None:
        """Posse confirmada pelo coordenador (RF14): aceita leituras e publica as guardadas, em ordem.

        Chamada também pela posse retida que chega a cada reconexão ao broker.
        """
        if not self._posse_confirmada.is_set() and self._pendentes:
            logging.info(
                f"Pulseira: {self.endereco} | Posse confirmada | Publicando {len(self._pendentes)} "
                f"leitura(s) de frequência cardíaca guardada(s) | Descartadas: {self._descartadas}"
            )
        self._descartadas = 0
        self._com_posse = True
        self._posse_confirmada.set()


    def perder_posse(self) -> None:
        """Sem a posse, descarta as leituras guardadas e ignora as que ainda chegarem (RF17)."""
        if self._pendentes:
            logging.warning(
                f"Pulseira: {self.endereco} | Sem a posse | "
                f"{len(self._pendentes)} leitura(s) de frequência cardíaca descartada(s)"
            )
        self._com_posse = False
        self._posse_confirmada.clear()
        self._pendentes.clear()
        self._descartadas = 0


    def suspender(self) -> None:
        """Chamada quando a sessão com o broker cai: guarda as leituras até a posse ser confirmada de novo."""
        self._posse_confirmada.clear()


    async def manter_publicacao(self) -> None:
        """Publica as leituras na ordem em que chegaram, até a tarefa ser cancelada."""
        while True:
            # Confere o estado a cada volta: o wait() de um Event retorna mesmo se o evento for limpo
            # antes de a tarefa retomar (posse confirmada e broker caindo no mesmo ciclo do loop)
            if not self._posse_confirmada.is_set():
                await self._posse_confirmada.wait()
            elif not self._pendentes:
                self._nova_leitura.clear()
                await self._nova_leitura.wait()
            else:
                await self._publicar_mais_antiga()


    async def publicar_pendentes(self) -> None:
        """Publica na hora as leituras guardadas, na ordem em que chegaram.

        Usada antes da liberação da posse, para nenhuma leitura deste coletor chegar depois das do
        novo dono (RF06). Sem a posse confirmada, não publica.
        """
        while self._pendentes and self._posse_confirmada.is_set():
            await self._publicar_mais_antiga()


    def mensagem(self, leitura: LeituraFrequenciaCardiaca) -> Mensagem:
        """Conteúdo do RF15, com `rr` só quando a pulseira envia.

        O timestamp do envelope é o do recebimento no coletor (RF16), e não o da publicação: é por ele
        que o consumidor ordena e descarta as repetidas, inclusive as que saem do buffer.
        """
        conteudo: dict = {"pulseira": self.endereco, "coletor": self.broker.cliente_id, "bpm": leitura.bpm}
        if leitura.intervalos_rr_ms:
            conteudo["rr"] = list(leitura.intervalos_rr_ms)
        return Mensagem(
            tipo=TipoMensagem.FREQUENCIA_CARDIACA,
            origem=self.broker.cliente_id,
            timestamp=leitura.timestamp,
            conteudo=conteudo,
        )


    async def _publicar_mais_antiga(self) -> None:
        # Retira antes do await: uma leitura que chegue durante a publicação pode encher o buffer
        leitura = self._pendentes.popleft()
        try:
            await self.broker.publicar(Topicos.frequencia_cardiaca(self.endereco), self.mensagem(leitura))
        except BrokerIndisponivel as e:
            if self.broker.conectado:
                # Falha com a sessão de pé: a leitura é perdida, como qualquer QoS 0. Devolvê-la ao buffer
                # e esperar a posse prenderia a publicação, porque sem reconexão a posse retida não volta.
                logging.warning(
                    f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {self.endereco} | "
                    f"Frequência cardíaca não publicada: {e}"
                )
                return
            # Sem broker: a leitura volta para a frente do buffer (ela é a mais antiga; com o buffer
            # cheio, é a que seria descartada) e espera a posse retida da reconexão.
            if len(self._pendentes) < self._pendentes.maxlen:
                self._pendentes.appendleft(leitura)
            else:
                self._descartadas += 1
            self._posse_confirmada.clear()
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {self.endereco} | "
                f"Guardando até {self._pendentes.maxlen} leituras de frequência cardíaca até reconectar: {e}"
            )
