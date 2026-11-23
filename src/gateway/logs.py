import logging
import os
import time

# RNF10: cada linha traz horário em UTC (ISO 8601), nível, componente e identificador do nó.
FORMATO = "%(asctime)s %(levelname)s %(componente)s %(no)s %(message)s"

_no_atual = "-"


def definir_no(no: str) -> None:
    """Guarda o identificador do nó (coletor, coordenador ou painel) para as próximas linhas de log."""
    global _no_atual
    _no_atual = no


class FormatadorUTC(logging.Formatter):
    """Horário em UTC no padrão ISO 8601 com milissegundos (2026-11-23T18:20:05.123Z).

    O componente é o módulo que emitiu a linha e o nó é o definido por definir_no().
    """

    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        # Segundos e milissegundos saem do mesmo inteiro, para 0,9996 s não virar ".1000"
        total_ms = round(record.created * 1000)
        instante = time.strftime("%Y-%m-%dT%H:%M:%S", self.converter(total_ms // 1000))
        return f"{instante}.{total_ms % 1000:03d}Z"

    def format(self, record: logging.LogRecord) -> str:
        record.componente = record.module
        record.no = _no_atual
        return super().format(record)


def configurar_logs(nivel: str | None = None) -> None:
    """Configura o log da raiz. Sem `nivel`, lê LOG_LEVEL (padrão INFO)."""
    nivel = (nivel or os.getenv("LOG_LEVEL") or "INFO").upper()
    manipulador = logging.StreamHandler()
    manipulador.setFormatter(FormatadorUTC(FORMATO))
    logging.basicConfig(level=nivel, handlers=[manipulador], force=True)

    # Bibliotecas que ficam muito verbosas em DEBUG
    logging.getLogger("bleak").setLevel(logging.WARNING)
    logging.getLogger("aiomqtt").setLevel(logging.WARNING)
