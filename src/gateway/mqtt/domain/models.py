import json
import logging
from enum import Enum
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field

# Mudou a estrutura de alguma mensagem: incrementar (RF15).
# v2: sinal-de-vida em tópico próprio, id no handoff, handoff na liberação e amostras só na varredura.
VERSAO_FORMATO = 2

class TipoMensagem(Enum):
    PRESENCA            = "presenca"
    SINAL_DE_VIDA       = "sinal-de-vida"
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
    TipoMensagem.SINAL_DE_VIDA:       ConfigEntrega(qos=0, retida=False),
    TipoMensagem.ESTADO_PULSEIRA:     ConfigEntrega(qos=0, retida=False),
    TipoMensagem.LIBERACAO_POSSE:     ConfigEntrega(qos=1, retida=False),
    TipoMensagem.FREQUENCIA_CARDIACA: ConfigEntrega(qos=0, retida=False),
    TipoMensagem.POSSE:               ConfigEntrega(qos=1, retida=True),
    TipoMensagem.HANDOFF:             ConfigEntrega(qos=1, retida=False),
}

# Campos que o `conteudo` de cada tipo sempre tem (RF15). Os opcionais ficam de fora:
# `pulseira` na presença (só no coletor), `amostras` (só na varredura), `handoff` na
# liberação (só em resposta a um handoff) e `rr` (só quando a pulseira envia).
CAMPOS_POR_TIPO: dict[TipoMensagem, frozenset[str]] = {
    TipoMensagem.PRESENCA:            frozenset({"estado"}),
    TipoMensagem.SINAL_DE_VIDA:       frozenset(),
    TipoMensagem.ESTADO_PULSEIRA:     frozenset({"pulseira", "estado", "rssi"}),
    TipoMensagem.LIBERACAO_POSSE:     frozenset({"pulseira"}),
    TipoMensagem.FREQUENCIA_CARDIACA: frozenset({"pulseira", "coletor", "bpm"}),
    TipoMensagem.POSSE:               frozenset({"pulseira", "coletor"}),
    TipoMensagem.HANDOFF:             frozenset({"id", "pulseira", "origem"}),
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
        """Serializa a mensagem no formato padronizado (RF15)."""
        return json.dumps({
            "versao": self.versao,
            "tipo": self.tipo.value,
            "origem": self.origem,
            "timestamp": self.timestamp.isoformat(),
            "conteudo": self.conteudo,
        }, ensure_ascii=False)

    @classmethod
    def de_json(cls, payload: str | bytes, topico: str | None = None) -> "Mensagem | None":
        """Interpreta uma mensagem recebida do broker (RF15).

        Args:
            payload (str | bytes): Corpo da mensagem.
            topico (str | None): Tópico de onde veio, só para o log do descarte.

        Returns:
            Mensagem | None: None se a mensagem for inválida ou de versão incompatível,
                que é descartada e registrada em log.
        """
        descarte = "Mensagem descartada" + (f" | Tópico: {topico}" if topico else "")
        try:
            dados = json.loads(payload)
            # A versão é conferida antes do resto: outra versão pode ter outra estrutura
            versao = dados["versao"]
            # type() e não isinstance(): bool é subclasse de int e 2.0 == 2
            if type(versao) is not int or versao != VERSAO_FORMATO:
                logging.warning(
                    f"{descarte} | Origem: {dados.get('origem')} | "
                    f"Motivo: versão {versao!r} incompatível (esperada {VERSAO_FORMATO})"
                )
                return None

            tipo = TipoMensagem(dados["tipo"])
            origem = dados["origem"]
            conteudo = dados["conteudo"]
            timestamp = datetime.fromisoformat(dados["timestamp"])
            if not isinstance(origem, str) or not origem:
                raise TypeError(f"origem {origem!r} não é um identificador")
            if not isinstance(conteudo, dict):
                raise TypeError(f"conteudo é {type(conteudo).__name__}, esperado objeto JSON")
            if timestamp.utcoffset() != timedelta(0):
                raise ValueError(f"timestamp {dados['timestamp']!r} não está em UTC")
            if faltando := CAMPOS_POR_TIPO[tipo] - conteudo.keys():
                raise KeyError(f"conteudo de {tipo.value} sem {', '.join(sorted(faltando))}")

            return cls(tipo=tipo, origem=origem, conteudo=conteudo, versao=versao, timestamp=timestamp)
        except (ValueError, KeyError, TypeError) as e:
            logging.warning(f"{descarte} | Motivo: formato inválido [{type(e).__name__}]: {e}")
            return None


class EstadoPresenca(Enum):
    ONLINE  = "online"
    OFFLINE = "offline"
