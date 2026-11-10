from bluetooth.domain.models import EstadoConexao


class ClassificacaoConexao:
    """Estado da conexão do coletor com a pulseira (RF08), um só para a varredura e a conexão GATT.

    A varredura classifica desconectado e nao-encontrado; a conexão, conectado e desconectado.
    Compartilhando a mesma classificação, a passagem de uma para a outra sai no log com o estado anterior correto.
    """
    def __init__(self) -> None:
        self._atual = EstadoConexao.DESCONECTADO

    @property
    def atual(self) -> EstadoConexao:
        return self._atual

    def mudar(self, novo: EstadoConexao) -> EstadoConexao | None:
        """Aplica o novo estado.

        Returns:
            EstadoConexao | None: O estado anterior, se mudou; None se continua o mesmo
        """
        if novo is self._atual:
            return None
        anterior, self._atual = self._atual, novo
        return anterior
