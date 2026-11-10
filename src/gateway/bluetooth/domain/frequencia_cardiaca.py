

from bluetooth.domain.erros import LeituraInvalida, SemContato, SemLeitura
from bluetooth.domain.models import LeituraFrequenciaCardiaca


# Flags do primeri byte da Heart Rate Measurement (0x2A37), conforme a especificação GATT
FLAG_BPM_UINT16 = 0x01
FLAG_CONTATO_DETECTADO = 0x02
FLAG_CONTTO_SUPORTADO = 0x04
FLAG_ENERGIA_GASTA = 0x08
FLAG_INTERVALOS_RR = 0x10

UNIDADES_RR_POR_SEGUNDO = 1024


def interpretar_medicao(dados: bytes | bytearray) -> LeituraFrequenciaCardiaca:
    """Converte o valor notificado pela característica 0x2A37 numa leitura.

    Formato: flags (1 byte), bpm (1 ou 2 bytes), energia gasta (2 bytes, opcional) e 
    intervalos RR (2 bytes cada, em 1/1024 s, opcionais). Tudo little-endian (do menor endereço para o maior).

    Args:
        dados (bytes | bytearray): Valor recebido na notificação.
        
    Returns: LeituraFrequenciaCardiaca: bpm, intervalos RR em ms (vazio se a pulseira não enviar)
        e horário de recebimento.

    Raises: 
        SemContato: Se as flags indicarem ausência de contato com a pele.
        SemLeitura: Se o bpm for 0.
        LeituraInvaldia: Se os bytes não seguirem o formato da característica.
    """
    if not dados: 
        raise LeituraInvalida("medição vázia")

    flags = dados[0]
    tamanho_bpm = 2 if flags & FLAG_BPM_UINT16 else 1
    inicio_rr = 1 + tamanho_bpm + (2 if flags & FLAG_ENERGIA_GASTA else 0)
    if len(dados) < inicio_rr:
        raise LeituraInvalida(f"medição com {len(dados)} byte(s), esperado pelo menos {inicio_rr}")

    # O contato é conferido antes do bpm
    if flags & FLAG_CONTTO_SUPORTADO and not flags & FLAG_CONTATO_DETECTADO:
        raise SemContato("pulseira sem contato com a pele.")

    bpm = int.from_bytes(dados[1:1 + tamanho_bpm], "little")
    if bpm == 0:
        raise SemLeitura("bpm igual a 0")

    bytes_rr = dados[inicio_rr:] if flags & FLAG_INTERVALOS_RR else b""
    if len(bytes_rr) % 2:
        raise LeituraInvalida(f"intervalos RR com {len(bytes_rr)} byte(s), esperado número par")
    intervalos_rr_ms = tuple(
        int.from_bytes(bytes_rr[i:i + 2], "little") * 1000 / UNIDADES_RR_POR_SEGUNDO
        for i in range(0, len(bytes_rr), 2)
    )
    return LeituraFrequenciaCardiaca(bpm=bpm, intervalos_rr_ms=intervalos_rr_ms)
