from collections import deque
from statistics import median


LIMIAR_CONEXAO_FRACA_PADRAO = -78
LEITURAS_FRACAS_PADRAO      = 3
LEITURAS_NORMAIS_PADRAO     = 3


class ClassificacaoConexaoFraca:
    """Classifica a conexão do coletor ativo como conexao-fraca a partir do RSSI que ele publica (RF19).

    Entra em conexao-fraca com `leituras_fracas` leituras seguidas abaixo do limiar e volta ao normal
    quando a mediana das últimas `leituras_normais` leituras fica acima dele. Os critérios de entrada e
    de saída são diferentes de propósito: uma leitura boa isolada não tira a conexão de conexao-fraca.

    Regra pura, sem I/O: quem decide o que fazer na transição (iniciar o handoff, RF06) é o coordenador.
    """

    def __init__(
        self,
        limiar: int = LIMIAR_CONEXAO_FRACA_PADRAO,
        leituras_fracas: int = LEITURAS_FRACAS_PADRAO,
        leituras_normais: int = LEITURAS_NORMAIS_PADRAO,
    ) -> None:
        """
        Args:
            limiar (int): dBm; a leitura igual ao limiar não é fraca ("abaixo").
            leituras_fracas (int): Leituras seguidas abaixo do limiar para entrar em conexao-fraca.
            leituras_normais (int): Leituras seguidas cuja mediana, acima do limiar, volta ao normal.
        """
        self.limiar = limiar
        self.leituras_fracas = leituras_fracas
        self._fraca = False
        self._seguidas = 0
        self._recentes: deque[int] = deque(maxlen=leituras_normais)


    @property
    def fraca(self) -> bool:
        return self._fraca


    @property
    def mediana(self) -> float | None:
        """Mediana das últimas leituras feitas em conexao-fraca (a que decidiu a volta ao normal); None sem leitura."""
        return median(self._recentes) if self._recentes else None


    def registrar(self, rssi: int) -> bool:
        """Registra uma leitura de RSSI da conexão ativa.

        Returns:
            bool: True só na leitura que muda o estado (entrada em conexao-fraca ou volta ao normal).
        """
        if not self._fraca:
            self._seguidas = self._seguidas + 1 if rssi < self.limiar else 0
            if self._seguidas < self.leituras_fracas:
                return False
            self._fraca = True
            self._recentes.clear()
            return True

        self._recentes.append(rssi)
        if len(self._recentes) < self._recentes.maxlen or median(self._recentes) <= self.limiar:
            return False
        self._fraca = False
        self._seguidas = 0
        return True


    def reiniciar(self) -> None:
        """Volta ao estado normal sem histórico: a conexão mudou (outro dono, desconexão, handoff)."""
        self._fraca = False
        self._seguidas = 0
        self._recentes.clear()
