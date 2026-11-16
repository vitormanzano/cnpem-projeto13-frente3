import asyncio
import logging
import os
import time


PRAZO_SINAL_DE_VIDA_COORDENADOR_PADRAO = 15.0
INTERVALO_VERIFICACAO_COORDENADOR      = 1.0 # s entre as verificações do sinal de vida


def ler_prazo_sinal_de_vida_coordenador(prazo: float | None = None) -> float:
    """Resolve o tempo sem sinal de vida do coordenador até considerá-lo indisponível (RF19).

    Args:
        prazo (float | None): Segundos. Se omitido, é lido de PRAZO_SINAL_DE_VIDA_COORDENADOR (padrão de 15 s).

    Raises:
        ValueError: Se o prazo não for um número maior que zero.
    """
    if prazo is None:
        valor = os.getenv("PRAZO_SINAL_DE_VIDA_COORDENADOR") or str(PRAZO_SINAL_DE_VIDA_COORDENADOR_PADRAO)
        try:
            prazo = float(valor)
        except ValueError as e:
            raise ValueError(f"PRAZO_SINAL_DE_VIDA_COORDENADOR deve ser um número, recebido {valor!r}.") from e

    if prazo <= 0:
        raise ValueError("PRAZO_SINAL_DE_VIDA_COORDENADOR deve ser maior que zero.")
    return prazo


class VigiaCoordenador:
    """Acompanha, no coletor, se o coordenador está disponível: presença online e sinal de vida
    dentro do prazo (RF19).

    Só registra a indisponibilidade: o coletor não muda de comportamento sem o coordenador. Quem tem
    a posse continua conectado (e reconecta, RF05) e quem não tem continua sem conectar, porque posse
    e handoff só vêm do coordenador.
    """

    def __init__(self, prazo: float | None = None) -> None:
        """
        Args:
            prazo (float | None): Segundos sem sinal de vida até considerar o coordenador indisponível.
                Se omitido, é lido de PRAZO_SINAL_DE_VIDA_COORDENADOR (padrão de 15 s).

        Raises:
            ValueError: Se o prazo for inválido.
        """
        self.prazo = ler_prazo_sinal_de_vida_coordenador(prazo)
        self._ouviu = False # Alguma mensagem do coordenador desde o início
        self._offline = False # Presença offline (encerramento ou última vontade)
        self._sem_sinal = False
        self._ultima_mensagem = time.monotonic()
        self._ouvindo = False # Conectado ao broker: sem broker, o silêncio não diz nada do coordenador
        self._anunciado: bool | None = None # Último estado registrado em log


    @property
    def disponivel(self) -> bool:
        return self._ouviu and not self._offline and not self._sem_sinal


    def registrar_presenca(self, online: bool, agora: float | None = None) -> None:
        """Presença do coordenador (retida, com última vontade)."""
        self._ouviu = True
        self._offline = not online
        if online:
            self._renovar(agora)
        self._anunciar("presença offline")


    def registrar_sinal_de_vida(self, agora: float | None = None) -> None:
        """Sinal de vida do coordenador. Não desfaz uma presença offline: o online sai antes do sinal."""
        self._ouviu = True
        self._renovar(agora)
        self._anunciar("presença offline")


    def verificar(self, agora: float | None = None) -> None:
        """Considera o coordenador indisponível se passou do prazo sem sinal de vida."""
        agora = time.monotonic() if agora is None else agora
        if not self._ouvindo or self._sem_sinal or agora - self._ultima_mensagem <= self.prazo:
            return
        self._sem_sinal = True
        self._anunciar(f"sem sinal de vida há mais de {self.prazo:g} s")


    def reiniciar_contagem(self, agora: float | None = None) -> None:
        """A cada (re)conexão ao broker: o tempo sem broker não conta contra o coordenador."""
        self._ultima_mensagem = time.monotonic() if agora is None else agora
        self._ouvindo = True


    def pausar(self) -> None:
        """A sessão com o broker caiu: sem receber nada, o coordenador não pode ser dado como sem sinal."""
        self._ouvindo = False


    async def manter_verificacao(self) -> None:
        """Verifica o sinal de vida a cada INTERVALO_VERIFICACAO_COORDENADOR, até ser cancelada."""
        while True:
            await asyncio.sleep(INTERVALO_VERIFICACAO_COORDENADOR)
            self.verificar()


    def _renovar(self, agora: float | None) -> None:
        self._ultima_mensagem = time.monotonic() if agora is None else agora
        self._sem_sinal = False


    def _anunciar(self, motivo: str) -> None:
        """Registra em log só as mudanças de estado."""
        disponivel = self.disponivel
        if disponivel is self._anunciado:
            return
        self._anunciado = disponivel
        if disponivel:
            logging.info("Coordenador disponível")
        else:
            logging.warning(
                f"Coordenador indisponível | Motivo: {motivo} | "
                "Posse e conexão mantidas; nenhuma posse nova nem handoff até ele voltar"
            )
