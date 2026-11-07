class GatewayError(Exception):
    """Base de todos os erros do coletor"""
    codigo = "erro-desconhecido"


class BluetoothError(GatewayError):
    codigo = "erro-bluetooth"


class BluetoothIndisponivel(BluetoothError):
    """Adaptador desligado, ausente ou inacessível"""
    codigo = "bluetooth-indisponivel"


class PulseiraNaoEncontrada(BluetoothError):
    codigo = "pulseira-nao-encontrada"


class FalhaConexao(BluetoothError):
    codigo = "falha-conexao"


class BrokerError(GatewayError):
    codigo = "erro-broker"


class BrokerIndisponivel(BrokerError):
    codigo = "broker-indisponivel"


class RSSIIndisponivel(BluetoothError):
    codigo = "rssi-indisponivel"
