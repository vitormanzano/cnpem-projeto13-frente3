class Topicos:
    @staticmethod
    def presenca_coletor(coletor: str) -> str:
        """Coletor: Presença (RF12)"""
        return f"coletores/{coletor}/presenca"


    @staticmethod
    def extrair_presenca_coletor(topico: str) -> str | None:
        """Devolve o coletor de coletores/{coletor}/presenca, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "coletores" and partes[2] == "presenca":
            return partes[1]
        return None


    @staticmethod
    def sinal_de_vida_coletor(coletor: str) -> str:
        """Coletor: Sinal de vida, sem retenção (RF12)"""
        return f"coletores/{coletor}/sinal-de-vida"


    @staticmethod
    def extrair_sinal_de_vida_coletor(topico: str) -> str | None:
        """Devolve o coletor de coletores/{coletor}/sinal-de-vida, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "coletores" and partes[2] == "sinal-de-vida":
            return partes[1]
        return None


    @staticmethod
    def estado_rssi(coletor: str, pulseira: str) -> str:
        """Coletor: Estado e RSSI"""
        return f"coletores/{coletor}/pulseiras/{pulseira}"


    @staticmethod
    def extrair_estado_rssi(topico: str) -> tuple[str, str] | None:
        """Devolve (coletor, pulseira) de coletores/{coletor}/pulseiras/{pulseira}, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 4 and partes[0] == "coletores" and partes[2] == "pulseiras":
            return partes[1], partes[3]
        return None


    @staticmethod
    def liberacao_posse(pulseira: str) -> str:
        """Coletor: Liberação da posse"""
        return f"pulseiras/{pulseira}/liberacao"


    @staticmethod
    def extrair_liberacao_posse(topico: str) -> str | None:
        """Devolve a pulseira de pulseiras/{pulseira}/liberacao, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "pulseiras" and partes[2] == "liberacao":
            return partes[1]
        return None


    @staticmethod
    def frequencia_cardiaca(pulseira: str) -> str:
        """Coletor ativo: Leituras de frequência cardíaca"""
        return f"pulseiras/{pulseira}/frequencia-cardiaca"


    @staticmethod
    def extrair_frequencia_cardiaca(topico: str) -> str | None:
        """Devolve a pulseira de pulseiras/{pulseira}/frequencia-cardiaca, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "pulseiras" and partes[2] == "frequencia-cardiaca":
            return partes[1]
        return None


    @staticmethod
    def estado_posse(pulseira: str) -> str:
        """Coordenador: Coletor responsável ou posse liberada"""
        return f"pulseiras/{pulseira}/posse"


    @staticmethod
    def extrair_estado_posse(topico: str) -> str | None:
        """Devolve a pulseira de pulseiras/{pulseira}/posse, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "pulseiras" and partes[2] == "posse":
            return partes[1]
        return None


    @staticmethod
    def handoff(pulseira: str) -> str:
        """Coordenador: Comando de handoff para a origem liberar a pulseira."""
        return f"pulseiras/{pulseira}/handoff"


    @staticmethod
    def extrair_handoff(topico: str) -> str | None:
        """Devolve a pulseira de pulseiras/{pulseira}/handoff, ou None se o tópico for outro."""
        partes = topico.split("/")
        if len(partes) == 3 and partes[0] == "pulseiras" and partes[2] == "handoff":
            return partes[1]
        return None


    @staticmethod
    def presenca_coordenador() -> str:
        """Coordenador: Presença (RF19).

        OBS: Esse método não tem nenhum ID pois, no momento atual, existe apenas 1 coordenador.
        """
        return "coordenador/presenca"


    @staticmethod
    def sinal_de_vida_coordenador() -> str:
        """Coordenador: Sinal de vida, sem retenção (RF19)"""
        return "coordenador/sinal-de-vida"
