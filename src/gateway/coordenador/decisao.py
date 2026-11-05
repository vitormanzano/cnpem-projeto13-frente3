from gateway.coordenador.visao import SinalColetor

MARGEM_HISTERESE_PADRAO = 5.0

def escolher_coletor(
    sinais: dict[str, SinalColetor],
    atual: str | None = None,
    margem: float = MARGEM_HISTERESE_PADRAO,
) -> str | None:
    """Escolhe o coletor de maior RSSI suavizada, no empate, o de menor id.

    Se `atual` estiver entre os candidatos, só perde para quem for melhor por pelo menos `margem` dB (evitar ping-pong).
    """
    if not sinais:
        return None
    melhor = min(sinais, key=lambda c: (-sinais[c].rssi_suavizada, c))
    if atual in sinais and melhor != atual:
        if sinais[melhor].rssi_suavizada - sinais[atual].rssi_suavizada < margem: # Evitar ping-pong
            return atual
    return melhor
