from enum import Enum


# Faixas do RF20. A divisa entre fraco e crítico não fica aqui: é o limiar de conexao-fraca do coordenador (RF19)
LIMITE_OTIMO = -60
LIMITE_BOM   = -70


class QualidadeSinal(Enum):
    OTIMO   = "otimo"
    BOM     = "bom"
    FRACO   = "fraco"
    CRITICO = "critico"


def classificar_sinal(rssi: float | None, limiar: int) -> QualidadeSinal | None:
    """Classifica o RSSI exibido pelo painel (RF20).

    Ótimo: >= -60 dBm. Bom: de -60 a -70. Fraco: de -70 até o limiar. Crítico: abaixo do limiar.
    O crítico é conferido primeiro e usa a mesma comparação do coordenador (`rssi < limiar`, RF19):
    se o limiar for configurado acima de -70, o painel continua mostrando como crítico tudo o que
    o coordenador conta como leitura fraca.

    Args:
        rssi (float | None): RSSI em dBm, ou None sem leitura.
        limiar (int): Limiar de conexao-fraca em dBm (padrão do coordenador: -78).

    Returns:
        QualidadeSinal | None: None se não houver leitura.
    """
    if rssi is None:
        return None
    if rssi < limiar:
        return QualidadeSinal.CRITICO
    if rssi >= LIMITE_OTIMO:
        return QualidadeSinal.OTIMO
    if rssi >= LIMITE_BOM:
        return QualidadeSinal.BOM
    return QualidadeSinal.FRACO
