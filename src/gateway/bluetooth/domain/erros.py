from types import ClassMethodDescriptorType


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


class ServicoIndisponviel(BluetoothError):
    """Serviço ou característica GATT ausente, ou notificações recusadas pela pulseira."""
    codigo = "servico-indisponivel"


class LeituraInvalida(BluetoothError):
    """Medição de frequência cardíaca fora do formato da característica 0x2A37."""
    codigo = "leitura-invalida"


class SemLeitura(LeituraInvalida):
    """bpm igual a 0: a pulseira não mediu batimento."""
    codigo = "sem-leitura"


class SemContato(LeituraInvalida):
    """Flags da medição indicam ausência de contato com a pele"""
