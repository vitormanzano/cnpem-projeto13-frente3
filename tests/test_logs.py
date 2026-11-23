"""Testes do formato dos logs (RNF10), sem hardware.

Rodar na raiz do projeto:  python tests/test_logs.py
(também funciona com pytest)
"""
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "gateway"))

import logs  # noqa: E402

LINHA = re.compile(
    r"^(?P<hora>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) (?P<nivel>[A-Z]+) (?P<componente>\S+) (?P<no>\S+) (?P<mensagem>.*)$"
)


def formatar(mensagem: str = "teste", nivel: int = logging.INFO, criado: float | None = None) -> str:
    registro = logging.LogRecord("raiz", nivel, __file__, 1, mensagem, None, None)
    if criado is not None:
        registro.created = criado
    return logs.FormatadorUTC(logs.FORMATO).format(registro)


def test_linha_tem_hora_nivel_componente_no_e_mensagem() -> None:
    logs.definir_no("coletor-a")
    achado = LINHA.match(formatar("Estado: conectado", logging.WARNING))
    assert achado is not None
    assert achado["nivel"] == "WARNING"
    assert achado["componente"] == "test_logs"
    assert achado["no"] == "coletor-a"
    assert achado["mensagem"] == "Estado: conectado"


def test_hora_em_utc_iso_8601_com_milissegundos() -> None:
    # 2026-11-23 18:20:05.123 UTC
    linha = formatar(criado=1795458005.123)
    assert linha.startswith("2026-11-23T18:20:05.123Z ")


def test_hora_nao_depende_do_fuso_da_maquina() -> None:
    import os
    import time

    if not hasattr(time, "tzset"):  # o Windows não tem tzset: este teste só roda em Linux e macOS
        return

    fuso_original = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Sao_Paulo"
        time.tzset()
        assert formatar(criado=1795458005.0).startswith("2026-11-23T18:20:05.000Z ")
    finally:
        if fuso_original is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = fuso_original
        time.tzset()


def test_milissegundos_nao_estouram_o_segundo() -> None:
    linha = formatar(criado=1795458005.9996)
    assert linha.startswith("2026-11-23T18:20:06.000Z ")


def test_no_padrao_e_traco_ate_ser_definido() -> None:
    logs.definir_no("-")
    assert LINHA.match(formatar())["no"] == "-"


def test_configurar_logs_aplica_o_nivel_e_silencia_bibliotecas() -> None:
    raiz = logging.getLogger()
    nivel_original, manipuladores = raiz.level, list(raiz.handlers)
    try:
        logs.configurar_logs("debug")
        assert raiz.level == logging.DEBUG
        assert isinstance(raiz.handlers[0].formatter, logs.FormatadorUTC)
        assert logging.getLogger("bleak").level == logging.WARNING
        assert logging.getLogger("aiomqtt").level == logging.WARNING
    finally:
        raiz.handlers[:] = manipuladores
        raiz.setLevel(nivel_original)
        logs.definir_no("-")


def test_configurar_logs_le_log_level_do_ambiente() -> None:
    import os

    raiz = logging.getLogger()
    nivel_original, manipuladores = raiz.level, list(raiz.handlers)
    anterior = os.environ.get("LOG_LEVEL")
    try:
        os.environ["LOG_LEVEL"] = "error"
        logs.configurar_logs()
        assert raiz.level == logging.ERROR
    finally:
        if anterior is None:
            os.environ.pop("LOG_LEVEL", None)
        else:
            os.environ["LOG_LEVEL"] = anterior
        raiz.handlers[:] = manipuladores
        raiz.setLevel(nivel_original)


if __name__ == "__main__":
    testes = [(nome, f) for nome, f in sorted(globals().items()) if nome.startswith("test_") and callable(f)]
    for nome, funcao in testes:
        funcao()
        print(f"ok  {nome}")
    print(f"\n{len(testes)} testes passaram.")
