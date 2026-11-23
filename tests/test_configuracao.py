"""Testes da configuração do projeto (docker-compose.yml e .env.example), sem hardware.

Rodar na raiz do projeto:  python tests/test_configuracao.py
(também funciona com pytest)
"""
import re
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parent.parent
SERVICOS = {"hivemq", "coordenador", "painel", "coletor"}

# Definidas pelo docker-compose.yml em cada serviço, e não pelo .env
VARIAVEIS_DO_COMPOSE = {"PAPEL"}


def carregar_compose() -> dict:
    return yaml.safe_load((RAIZ / "docker-compose.yml").read_text(encoding="utf-8"))


def variaveis_lidas_no_codigo() -> set[str]:
    """Nomes passados como literal a os.getenv(...) em qualquer arquivo de src/."""
    nomes: set[str] = set()
    for arquivo in (RAIZ / "src").rglob("*.py"):
        nomes |= set(re.findall(r'os\.getenv\(\s*"([A-Z][A-Z0-9_]*)"', arquivo.read_text(encoding="utf-8")))
    return nomes


def variaveis_do_env_example() -> set[str]:
    """Nomes definidos (ou comentados como exemplo) no .env.example."""
    texto = (RAIZ / ".env.example").read_text(encoding="utf-8")
    return set(re.findall(r"^#?\s*([A-Z][A-Z0-9_]*)=", texto, flags=re.MULTILINE))


def test_compose_tem_os_quatro_servicos() -> None:
    assert set(carregar_compose()["services"]) == SERVICOS


def test_compose_separa_central_e_coletor_em_perfis() -> None:
    servicos = carregar_compose()["services"]
    for nome in ("hivemq", "coordenador", "painel"):
        assert servicos[nome]["profiles"] == ["central"], nome
    assert servicos["coletor"]["profiles"] == ["coletor"]


def test_compose_define_o_papel_de_cada_servico_da_aplicacao() -> None:
    servicos = carregar_compose()["services"]
    for papel in ("coordenador", "painel", "coletor"):
        assert servicos[papel]["environment"]["PAPEL"] == papel


def test_compose_coletor_acessa_o_bluetooth_do_host() -> None:
    """O coletor precisa da rede e do D-Bus do host para falar com o BlueZ."""
    coletor = carregar_compose()["services"]["coletor"]
    assert coletor["network_mode"] == "host"
    assert coletor["privileged"] is True
    assert any("system_bus_socket" in volume for volume in coletor["volumes"])


def test_compose_rotaciona_os_logs_com_ate_100_mb_por_no() -> None:
    """RNF07: sem rotação, o log do Docker cresce até encher o cartão SD."""
    for nome, servico in carregar_compose()["services"].items():
        logging = servico.get("logging")
        assert logging, f"{nome} sem rotação de logs"
        opcoes = logging["options"]
        tamanho_mb = int(opcoes["max-size"].rstrip("m"))
        assert tamanho_mb * int(opcoes["max-file"]) <= 100, nome


def test_toda_variavel_lida_no_codigo_esta_no_env_example() -> None:
    faltando = variaveis_lidas_no_codigo() - variaveis_do_env_example() - VARIAVEIS_DO_COMPOSE
    assert not faltando, f"Variáveis lidas no código e ausentes do .env.example: {sorted(faltando)}"


def test_prazo_de_conexao_igual_nas_duas_pontas() -> None:
    """RF14: o mesmo PRAZO_CONEXAO no coletor e na central, senão o handoff decide com prazos diferentes."""
    texto = (RAIZ / ".env.example").read_text(encoding="utf-8")
    valores = re.findall(r"^PRAZO_CONEXAO=(\S+)", texto, flags=re.MULTILINE)
    assert len(valores) == 2 and len(set(valores)) == 1, valores


if __name__ == "__main__":
    testes = [(nome, f) for nome, f in sorted(globals().items()) if nome.startswith("test_") and callable(f)]
    for nome, funcao in testes:
        funcao()
        print(f"ok  {nome}")
    print(f"\n{len(testes)} testes passaram.")
