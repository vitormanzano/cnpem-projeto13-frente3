from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

class EstadoConexao(Enum):
    CONECTADO = "conectado"
    DESCONECTADO = "desconectado"
    CONEXAO_FRACA = "conexao-fraca"
    NAO_ENCONTRADO = "nao-encontrado"


@dataclass(frozen=True)
class Pulseira:
    endereco: str
    nome: str | None
    rssi: int | None = None


@dataclass(frozen=True)
class DispositivoBLE:
    """Dispositivo detectado anunciando durante uma varredura"""
    endereco: str
    nome: str
    rssi: int


@dataclass(frozen=True)
class LeituraRSSI:
    endereco: str
    estado: EstadoConexao
    rssi: int | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Servico:
    uuid: str 
    descricao: str


@dataclass(frozen=True)
class LeituraFrequenciaCardiaca:
    bpm: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
