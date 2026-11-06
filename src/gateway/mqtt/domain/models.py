import json
import logging
from enum import Enum
from datetime import datetime, timezone
from dataclasses import dataclass, field

VERSAO_FORMATO = 1

class TipoMensagem(Enum):
    PRESENCA            = "presenca"
    ESTADO_PULSEIRA     = "estado-pulseira"
    LIBERACAO_POSSE     = "liberacao-posse"
    FREQUENCIA_CARDIACA = "frequencia-cardiaca"
    POSSE               = "posse"
    HANDOFF             = "handoff"


@dataclass(frozen=True)
class ConfigEntrega:
    """Nível de entrega (QoS) e retenção usados ao publicar um tipo de mensagem."""
    qos: int
    retida: bool


CONFIG_POR_TIPO: dict[TipoMensagem, ConfigEntrega] = {
    TipoMensagem.PRESENCA:            ConfigEntrega(qos=1, retida=True),
    TipoMensagem.ESTADO_PULSEIRA:     ConfigEntrega(qos=0, retida=False),
    TipoMensagem.LIBERACAO_POSSE:     ConfigEntrega(qos=1, retida=False),
    TipoMensagem.FREQUENCIA_CARDIACA: ConfigEntrega(qos=0, retida=False),
    TipoMensagem.POSSE:               ConfigEntrega(qos=1, retida=True),
    TipoMensagem.HANDOFF:             ConfigEntrega(qos=1, retida=False),
}


@dataclass(frozen=True)
class Mensagem:
    tipo: TipoMensagem
    origem: str # Id coletor ou id coordenador
    conteudo: dict
    versao: int = VERSAO_FORMATO
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def config_entrega(self) -> ConfigEntrega:
        """QoS e retenção definidos para o tipo desta mensagem."""
        return CONFIG_POR_TIPO[self.tipo]

    def para_json(self) -> str:
        """Serializa a mensagem no formato padronizado"""
        return json.dumps({
            "versao": self.versao,
            "tipo": self.tipo.value,
            "origem": self.origem,
            "timestamp": self.timestamp.isoformat(),
            "conteudo": self.conteudo,
        }, ensure_ascii=False)

    @classmethod
    def de_json(cls, payload: str | bytes) -> "Mensagem | None":
        """Interpreta uma mensagem recebida do broker.

        Returns:
            Mensagem | None: None se a mensagem for inválida ou de versão incompatível,
                que é descartada e registrada em log.
        """
        try:
            dados = json.loads(payload)
            versao = dados["versao"]
            if versao != VERSAO_FORMATO:
                logging.warning(
                    f"Mensagem descartada | versão {versao} incompatível (esperada {VERSAO_FORMATO})"
                )
                return None

            return cls(
                tipo=TipoMensagem(dados["tipo"]),
                origem=dados["origem"],
                conteudo=dados["conteudo"],
                versao=versao,
                timestamp=datetime.fromisoformat(dados["timestamp"]),
            )
        except (ValueError, KeyError, TypeError) as e:
            logging.warning(f"Mensagem descartada | formato inválido [{type(e).__name__}]: {e}")
            return None


class EstadoPresenca(Enum):
    ONLINE  = "online"
    OFFLINE = "offline"
