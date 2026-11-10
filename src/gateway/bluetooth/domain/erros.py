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
    """Leitura de RSSI da conexão ativa falhou (tempo esgotado ou resposta sem RSSI)."""
    codigo = "rssi-indisponivel"


class PlataformaSemRSSI(RSSIIndisponivel):
    """A plataforma não permite ler o RSSI da conexão (sem btmgmt ou sem permissão): repetir não resolve."""


class ServicoIndisponivel(BluetoothError):
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
    codigo = "sem-contato"
