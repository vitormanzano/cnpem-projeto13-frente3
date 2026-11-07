from coordenador.visao import SinalColetor

MARGEM_HISTERESE_PADRAO = 5.0
MINIMO_AMOSTRAS_PADRAO  = 5 # Talvez precise de ajuste (medir a quantidade de amostras que chega)

def escolher_coletor(
    sinais: dict[str, SinalColetor],
    atual: str | None = None,
    margem: float = MARGEM_HISTERESE_PADRAO,
    minimo_amostras: int = MINIMO_AMOSTRAS_PADRAO
) -> str | None:
    """Escolhe o coletor de maior RSSI suavizada, no empate, o de menor id.

    Se `atual` estiver entre os candidatos, só perde para quem for melhor por pelo menos `margem` dB (evitar ping-pong).
    """
    if not sinais:
        return None
    melhor = min(sinais, key=lambda c: (-sinais[c].rssi_suavizada, c))
    if atual in sinais and melhor != atual:
        desafiante, favorito = sinais[melhor], sinais[atual]
        if ( (desafiante.rssi_suavizada - favorito.rssi_suavizada) < margem
            or desafiante.total_amostras < minimo_amostras):
            return atual
    return melhor
