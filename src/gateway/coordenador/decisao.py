import math

from coordenador.visao import Candidato


MARGEM_HISTERESE_PADRAO = 5.0
# Médias ponderadas iguais podem diferir no último bit conforme a ordem das somas
TOLERANCIA_RSSI = 1e-9


def escolher_coletor(
    candidatos: dict[str, Candidato],
    favorito: str | None = None,
    margem: float = MARGEM_HISTERESE_PADRAO,
) -> str | None:
    """Escolhe o vencedor da disputa pela posse (RF14).

    Vence o maior RSSI suavizado. No empate, vence o favorito; se ele não estiver no empate,
    vence quem publicou primeiro na janela. O favorito que tem sinal válido na janela só perde
    para quem for maior por pelo menos `margem` dB; sem sinal válido, vence o melhor, sem margem.

    Args:
        candidatos (dict[str, Candidato]): Coletores com sinal válido na janela.
        favorito (str | None): Último coletor ativo da pulseira.
        margem (float): Vantagem, em dB, que um candidato precisa ter sobre o favorito.

    Returns:
        str | None: O coletor vencedor, ou None sem candidatos.
    """
    if not candidatos:
        return None

    maior = max(candidato.rssi_suavizado for candidato in candidatos.values())
    empatados = [
        coletor for coletor, candidato in candidatos.items()
        if math.isclose(candidato.rssi_suavizado, maior, abs_tol=TOLERANCIA_RSSI)
    ]
    if favorito in empatados:
        return favorito
    # O horário de chegada ao coordenador, e não o timestamp do envelope: cada coletor tem o seu relógio
    melhor = min(empatados, key=lambda coletor: candidatos[coletor].primeira_em)

    if favorito in candidatos:
        vantagem = candidatos[melhor].rssi_suavizado - candidatos[favorito].rssi_suavizado
        if vantagem + TOLERANCIA_RSSI < margem:
            return favorito
    return melhor
