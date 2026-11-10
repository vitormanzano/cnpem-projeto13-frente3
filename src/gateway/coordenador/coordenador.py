import asyncio
import os
import logging
import time
import uuid

import aiomqtt
from bluetooth.domain.erros import BrokerIndisponivel
from bluetooth.domain.models import EstadoConexao
from coordenador.visao import SinalColetor, VisaoColetores
from mqtt.conexao import ConexaoBroker
from mqtt.domain.models import EstadoPresenca, Mensagem, TipoMensagem
from mqtt.domain.topicos import Topicos
from coordenador.decisao import MARGEM_HISTERESE_PADRAO, MINIMO_AMOSTRAS_PADRAO, escolher_coletor


INTERVALO_SINAL_DE_VIDA_PADRAO = 5.0
JANELA_DISPUTA_PADRAO          = 5.0
JANELA_HANDOFF_PADRAO          = 3.0
FATOR_EXTENSAO_JANELA_PADRAO   = 3   # Sem leituras, a janela vai até a janela * fator
INTERVALO_VERFICACAO           = 0.5 # s entre verificações durante a extensão
LIMIAR_CONEXAO_FRACA_PADRAO    = -78
LEITURAS_FRACAS_PADRAO         = 3
PRAZO_LIBERACAO_PADRAO         = 3.0 # s para a origem confirmar que soltou


def ler_intervalo_sinal_de_vida(intervalo: float | None = None) -> float:
    """Resolve o intervalo de sinal de vida.

    Se omitido, é lido da variável de ambiente PRESENCA_INTERVALO_COORDENADOR (padrão de 5 s).

    Raises:
        ValueError: Se o intervalo não for positivo ou a variável de ambiente não for um número.
    """
    if intervalo is None:
        intervalo = float(os.getenv("PRESENCA_INTERVALO_COORDENADOR", INTERVALO_SINAL_DE_VIDA_PADRAO))

    if intervalo <= 0:
        raise ValueError("PRESENCA_INTERVALO_COORDENADOR deve ser maior que zero.")
    return intervalo


class Coordenador:
    """Responsável por gerenciar as conexões entre coletores e pulseiras."""

    def __init__(self, broker: "ConexaoBroker", intervalo: float | None = None) -> None:
        """Registra sua presença e coordena os coletores.
            Args:
                broker (ConexaoBroker): Sessão MQTT do coletor.
                intervalo (float | None): Segundos entre os sinais de vida. Se omitido, é lido 
                    de PRESENCA_INTERVALO (padrão de 5 s).

            Raises:
                ValueError: Se o intervalo for válido.
        """
        self.broker:           ConexaoBroker            = broker
        self.intervalo:        float                    = ler_intervalo_sinal_de_vida(intervalo)
        self.visao                                      = VisaoColetores()
        self._encerrada                                 = False
        
        self.broker.ultima_vontade                      = self._ultima_vontade()
        self.broker.adicionar_ao_conectar(self.anunciar_online)
        self.broker.ao_receber                          = self.ao_receber

        self.janela_disputa                             = float(os.getenv("JANELA_DISPUTA", JANELA_DISPUTA_PADRAO))
        self.janela_handoff                             = float(os.getenv("JANELA_HANDOFF", JANELA_HANDOFF_PADRAO))
        self.limiar                                     = int(os.getenv("LIMIAR_CONEXAO_FRACA", LIMIAR_CONEXAO_FRACA_PADRAO))
        self.minimo_fracas                              = int(os.getenv("LEITURAS_FRACAS", LEITURAS_FRACAS_PADRAO))
        self.prazo_liberacao                            = float(os.getenv("PRAZO_LIBERACAO", PRAZO_LIBERACAO_PADRAO))
        self.margem                                     = float(os.getenv("MARGEM_HISTERESE", MARGEM_HISTERESE_PADRAO))
        self.minimo_amostras                            = int(os.getenv("MINIMO_AMOSTRAS", MINIMO_AMOSTRAS_PADRAO))
        self.fator_extensao                             = float(os.getenv("FATOR_EXTENSAO_JANELA", FATOR_EXTENSAO_JANELA_PADRAO))
        
        self._liberacoes:      dict[str, asyncio.Event] = {} # Pulseira -> handoff aguardando a origem
        self._disputas:        dict[str, asyncio.Task]  = {} # Pulseira -> Janela em andamento
        self._leituras_fracas: dict[str, int]           = {} # Pulseira -> leituras fracas seguidas do dono
        self._ultimo_ativo:    dict[str, str]           = {} # Pulseira -> último coletor conectado (favorito)


    async def assinar_topicos(self) -> None:
        """Assina, presença, sinal e posse de todos os coletores e pulseiras.

        Pode ser chamado antes de conectar: a ConexaoBroker refaz as assinaturas de cada
        (re)conexão, e o broker entrega na hora as mensagens retidas de presença e posse.
        """
        await self.broker.assinar(Topicos.presenca_coletor("+"))
        await self.broker.assinar(Topicos.estado_rssi("+", "+"), qos=0)
        await self.broker.assinar(Topicos.estado_posse("+"))
        await self.broker.assinar(Topicos.liberacao_posse("+"))

    
    async def ao_receber(self, topico: str, mensagem: Mensagem) -> None:
        """Encaminha cada mensagem pelo tipo; o tópico só é usado para extrair os ids."""
        match mensagem.tipo:
            case TipoMensagem.PRESENCA:
                await self._tratar_presenca(topico, mensagem)
            case TipoMensagem.ESTADO_PULSEIRA:
                await self._tratar_sinal(topico, mensagem)
            case TipoMensagem.POSSE:
                await self._tratar_posse(topico, mensagem)
            case TipoMensagem.LIBERACAO_POSSE:
                await self._tratar_liberacao(topico, mensagem)


    async def _tratar_presenca(self, topico: str, mensagem: Mensagem) -> None:
        coletor = Topicos.extrair_presenca_coletor(topico)
        if coletor is None:
            return

        online = mensagem.conteudo.get("estado") == EstadoPresenca.ONLINE.value
        # Só registra mudança: a presença também é republicada a cada reconexão e troca de pulseira
        if self.visao.atualizar_presenca(coletor, online):
            estado = EstadoPresenca.ONLINE if online else EstadoPresenca.OFFLINE
            logging.info(f"Presença | Coletor: {coletor} | {estado.value}")

        if not online:
            for pulseira in self.visao.pulseiras_de(coletor):
                await self._liberar_posse_orfa(pulseira, coletor)


    async def _tratar_posse(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_estado_posse(topico)
        if pulseira is None:
            return
        dono = mensagem.conteudo.get("coletor")
        self.visao.atualizar_posse(pulseira, dono)

        # Posse retida em um coletor que já caiu (ex: coordenador reiniciou)
        if dono is not None and dono in self.visao.coletores_offline:
            await self._liberar_posse_orfa(pulseira, dono)


    async def _liberar_posse_orfa(self, pulseira: str, coletor: str) -> None:
        """Coletor responsável ficou indisponível, então a posse é liberada."""
        if (liberada := self._liberacoes.get(pulseira)) is not None:
            # A queda vale como liberação para o handoff
            self.visao.limpar_amostras(pulseira)
            liberada.set()
            return

        logging.info(f"Posse liberada | Pulseira: {pulseira} | Motivo: coletor {coletor} offline.")
        self._leituras_fracas.pop(pulseira, None)
        try:
            await self._publicar_posse(pulseira, None)
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {pulseira} | Posse não liberada: {e}")


    async def _tratar_sinal(self, topico: str, mensagem: Mensagem) -> None:
        ids = Topicos.extrair_estado_rssi(topico)
        if ids is None: 
            return
        coletor, pulseira = ids

        if mensagem.origem != coletor:
            logging.warning(f"Sinal descartado | {topico} | origem {mensagem.origem} não é o coletor do tópico")
            return

        try:
            estado = EstadoConexao(mensagem.conteudo["estado"])
            rssi = mensagem.conteudo.get("rssi")
            rssi = None if rssi is None else int(rssi)
            amostras = int(mensagem.conteudo.get("amostras") or 1) # Leituras de conexão ativa vale 1
        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"Sinal descartado | {topico} | conteúdo inválido [{type(e).__name__}]: {e}")
            return
        if estado is EstadoConexao.CONEXAO_FRACA:
            # Quem classifica conexao-fraca é o coordenador (RF19); o coletor não publica esse estado (RF15)
            logging.warning(f"Sinal descartado | {topico} | estado {estado.value} não vem do coletor")
            return

        sinal = self.visao.registrar_sinal(pulseira, coletor, estado, rssi, amostras)
        if estado is EstadoConexao.CONECTADO:
            self._ultimo_ativo[pulseira] = coletor # Favorito
        logging.debug(
            f"Sinal | Pulseira: {pulseira} | Coletor: {coletor} | {estado.value} | "
            f"RSSI: {rssi} | suavizado: {sinal.rssi_suavizada}"
        )
        await self._avaliar(pulseira, coletor, sinal)


    async def _tratar_liberacao(self, topico: str, mensagem: Mensagem) -> None:
        pulseira = Topicos.extrair_liberacao_posse(topico)
        if pulseira is None:
            return
        if mensagem.origem != self.visao.posse.get(pulseira):
            logging.warning(f"Liberação descartada | Pulseira: {pulseira} | {mensagem.origem} não é o dono.")
            return

        handoff = mensagem.conteudo.get("handoff")
        logging.info(
            f"Liberação recebida | Pulseira: {pulseira} | Coletor: {mensagem.origem}"
            + (f" | Handoff: {handoff}" if handoff else "")
        )
        # Qualquer liberação do dono conta, mesmo sem o id: a pulseira foi solta de qualquer jeito
        if (evento := self._liberacoes.get(pulseira)) is not None:
            # Resposta ao HANDOFF. Limpa aqui, e não em _conduzir_handoff, para não apagar
            # leituras que cheguem entre esta mensagem e a retomada da tarefa.
            self.visao.limpar_amostras(pulseira)
            evento.set()
            return
        try:
            # Coletor encerrado de forma controlada
            await self._publicar_posse(pulseira, None)
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {pulseira} | Posse não liberada: {e}")


    async def _iniciar_handoff(self, pulseira: str, dono: str) -> None:
        """Soltar e disputar: Manda a origem soltar; o resto acontece em _conduzir_handoff.

        O id relaciona no log o comando, a liberação e o resultado (RF06).
        """
        id_handoff = uuid.uuid4().hex[:8] # Curto para o log; só precisa distinguir os handoffs entre si
        logging.info(
            f"Handoff iniciado | Id: {id_handoff} | Pulseira: {pulseira} | Origem: {dono} | Motivo: conexao-fraca "
            f"({self._leituras_fracas[pulseira]} leituras < {self.limiar} dBm)"
        )
        self._leituras_fracas[pulseira] = 0
        comando = Mensagem(
            tipo=TipoMensagem.HANDOFF,
            origem=self.broker.cliente_id,
            conteudo={
                "id": id_handoff,
                "pulseira": pulseira,
                "origem": dono,
            }
        )
        # Criado antes do HANDOFF: a liberação pode ser processada antes de a tarefa começar a rodar
        liberada = self._liberacoes[pulseira] = asyncio.Event()
        try:
            await self.broker.publicar(Topicos.handoff(pulseira), comando)
        except BrokerIndisponivel as e:
            self._liberacoes.pop(pulseira, None)
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Handoff não iniciado: {e}"
            )
            return
        self._disputas[pulseira] = asyncio.create_task(self._conduzir_handoff(pulseira, dono, liberada, id_handoff))


    async def _conduzir_handoff(self, pulseira: str, dono: str, liberada: asyncio.Event, id_handoff: str) -> None:
        """Espera a origem liberar, publica a posse como liberada e abre a disputa"""
        try:
            try:
                await asyncio.wait_for(liberada.wait(), self.prazo_liberacao)
            except TimeoutError:
                logging.warning(
                    f"Handoff cancelado | Id: {id_handoff} | Pulseira: {pulseira} | "
                    f"Origem {dono} não liberou em {self.prazo_liberacao:g} s; continua sendo dono."
                )
                return
            await self._publicar_posse(pulseira, None)
            logging.info(
                f"Disputa aberta | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Janela: {self.janela_handoff:g} s | Favorito: {self._ultimo_ativo.get(pulseira)}"
            )
            await self._encerrar_disputa(pulseira, self.janela_handoff, id_handoff)
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Handoff: {id_handoff} | Pulseira: {pulseira} | "
                f"Handoff interrompido: {e}"
            )
        finally:
            self._liberacoes.pop(pulseira, None)
            self._disputas.pop(pulseira, None)


    async def anunciar_online(self) -> None:
        """Publica a presença online (retida) no encerramento controlado."""
        if self._encerrada:
            return
        if await self._publicar_presenca(EstadoPresenca.ONLINE):
            logging.info(f"Presença: {EstadoPresenca.ONLINE.value} | Coordenador ")


    async def anunciar_offline(self) -> None:
        """Publica a presença offline (retida) no encerramento controlado.

        Deve ser chamado antes de desconectar do broker: numa desconexão limpa o broker
        descarta a última vontade, então este é o único aviso de offline. Depois dele, 
        o sinal de vida para e as reconexões não anunciam mais online.
        """
        self._encerrada = True
        if await self._publicar_presenca(EstadoPresenca.OFFLINE):
            logging.info(f"Presença: {EstadoPresenca.OFFLINE.value} | Coordenador ")


    def mensagem(self, estado: EstadoPresenca) -> Mensagem:
        """Monta a mensagem de presença. O timestamp vão no envelope."""
        return Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": estado.value,
            },
        )


    async def manter_sinal_de_vida(self) -> None:
        """Publica o sinal de vida em tópico próprio, sem retenção, a cada `intervalo` segundos (RF19),
        até o encerramento ou cancelamento.

        Sem conexão com o broker o sinal é pulado; a falha já é registrada pela ConexaoBroker.
        """
        while not self._encerrada:
            await asyncio.sleep(self.intervalo)
            if self._encerrada or not self.broker.conectado:
                continue

            sinal = Mensagem(tipo=TipoMensagem.SINAL_DE_VIDA, origem=self.broker.cliente_id, conteudo={})
            try:
                await self.broker.publicar(Topicos.sinal_de_vida_coordenador(), sinal)
                logging.debug("Sinal de vida | Coordenador")
            except BrokerIndisponivel as e:
                logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Coordenador | Sinal de vida não publicado: {e}")


    async def registrar_visao(self) -> None:
        """Loga em DEBUG, a cada 10 s, como o coordenador enxerga a rede."""
        while not self._encerrada:
            await asyncio.sleep(10)
            logging.debug("%s", self.visao)


    async def _publicar_presenca(self, estado: EstadoPresenca) -> bool:
        """Publica a presença. Devolve False se o broker estiver indisponível, sem lançar erro."""
        try:
            await self.broker.publicar(Topicos.presenca_coordenador(), self.mensagem(estado))
            return True
        except BrokerIndisponivel as e:
            logging.warning(
                f"Erro: {BrokerIndisponivel.codigo} | Coordenador: {self.broker.cliente_id} | "
                f"Não foi possível publicar a presença {estado.value}: {e}"
            )
            return False


    def _ultima_vontade(self) -> aiomqtt.Will:
        """Presença offline publicada pelo broker se o coordenador cair sem avisar.

        O timestamp é o do registro, não o da queda: para saber quando o coletor caiu,
        o coordenador deve usar o horário de recebimento.
        """
        mensagem = Mensagem(
            tipo=TipoMensagem.PRESENCA,
            origem=self.broker.cliente_id,
            conteudo={
                "estado": EstadoPresenca.OFFLINE.value,
            },
        )
        config = mensagem.config_entrega
        return aiomqtt.Will(
                topic=Topicos.presenca_coordenador(),
                payload=mensagem.para_json(),
                qos=config.qos,
                retain=config.retida,
        )

    
    async def _avaliar(self, pulseira: str, coletor: str, sinal: SinalColetor) -> None:
        """Decide, a cada sinal, se a pulseira precisa de um coletor (novo ou outro)."""
        if pulseira in self._disputas:
            return # Em disputa de handoff

        dono = self.visao.posse.get(pulseira)
        if dono is None:
            if self.visao.sinais_validos(pulseira):
                self._abrir_disputa(pulseira, self.janela_disputa)
            return

        if coletor != dono or not self._conexao_fraca(pulseira, sinal):
            return # Só a leitura do dono dispara handoff


        # Sem comparação prévia: a pulseira conectada não anuncia, então nenhum vizinho tem sinal.
        await self._iniciar_handoff(pulseira, dono)
        
            
    def _conexao_fraca(self, pulseira: str, sinal: SinalColetor) -> bool: 
        """Conexao-fraca após N leituras consecutivas do dono abaixo do limiar."""
        if sinal.estado is EstadoConexao.CONECTADO and sinal.rssi is not None and sinal.rssi < self.limiar:
            fracas = self._leituras_fracas.get(pulseira, 0) + 1
        else:
            fracas = 0
        self._leituras_fracas[pulseira] = fracas
        if fracas == self.minimo_fracas:
            logging.info(f"Estado: conexao-fraca | Pulseira: {pulseira} | RSSI: {sinal.rssi} dBm.")
        return fracas >= self.minimo_fracas


    def _abrir_disputa(self, pulseira: str, janela: float) -> None:
        logging.info(f"Disputa aberta | Pulseira: {pulseira} | Janela: {janela:g} s | favorito: {self._ultimo_ativo.get(pulseira)}")
        self._disputas[pulseira] = asyncio.create_task(self._encerrar_disputa(pulseira, janela))


    async def _encerrar_disputa(self, pulseira: str, janela: float, handoff: str | None = None) -> None:
        """Ao fim da janela, concede a posse ao melhor sinal. O ultimo coletor ativo é o favorito:
        só perde para quem for melhor pela margem e tiver o mínimo de amostras.

        Args:
            handoff (str | None): Id do handoff que abriu a disputa, para o log do resultado (RF06).
        """
        ref = f" | Handoff: {handoff}" if handoff else ""
        try:
            await asyncio.sleep(janela)
            if self.visao.posse.get(pulseira) is not None:
                return # a posse retida chegou durante a janela (coordenador acabou de ligar)

            if not self.visao.sinais_validos(pulseira):
                if not await self._estender_janela(pulseira, janela):
                    logging.info(
                        f"Disputa sem vencedor{ref} | Pulseira: {pulseira} | Nenhum coletor a enxergou em "
                        f"{janela * self.fator_extensao:g} s; posse continua liberada."
                    )
                    return

                await asyncio.sleep(janela) # Janela completa a partir da primeira leitura.

            favorito = self._ultimo_ativo.get(pulseira)
            sinais = self.visao.sinais_validos(pulseira)

            vencedor = escolher_coletor(sinais, atual=favorito, margem=self.margem, minimo_amostras=self.minimo_amostras)
            if vencedor is None:
                logging.info(f"Disputa sem vencedor{ref} | Pulseira: {pulseira} | As leituras expiraram durante a janela.")
                return

            await self._publicar_posse(pulseira, vencedor)
            motivo = "favorito" if vencedor == favorito else "Maior RSSI"
            rssis = ", ".join(f"{c}: {s.rssi_suavizada:.1f} ({s.total_amostras} amostras)"  for c, s in sinais.items())
            logging.info(f"Posse concedida{ref} | Pulseira: {pulseira} | Coletor: {vencedor} | Motivo: {motivo} | RSSI [{rssis}]")
        except BrokerIndisponivel as e:
            logging.warning(f"Erro: {BrokerIndisponivel.codigo} | Pulseira: {pulseira} | Posse não publicada: {e}")
        finally:
            self._disputas.pop(pulseira, None)


    async def _publicar_posse(self, pulseira: str, coletor: str | None) -> None:
        """Publica a posse (retida) e já atualiza a visão, sem esperar o broker."""
        mensagem = Mensagem(
            tipo=TipoMensagem.POSSE,
            origem=self.broker.cliente_id,
            conteudo={
                "pulseira": pulseira,
                "coletor": coletor
            }
        )
        await self.broker.publicar(Topicos.estado_posse(pulseira), mensagem)
        self.visao.atualizar_posse(pulseira, coletor)


    async def _estender_janela(self, pulseira: str, janela: float) -> bool:
        """Espera a primeira leitura válida até a janela somar `fator_extensao` vezes o padrão

        Returns:
            bool: True se algum coletor enxergou a pulseira dentro do limite.
        """
        prazo = time.monotonic() + janela * (self.fator_extensao - 1) # Uma janela já passou
        while not self.visao.sinais_validos(pulseira):
            if time.monotonic() >= prazo:
                return False
            await asyncio.sleep(INTERVALO_VERFICACAO)
        return True
    

    async def executar(self) -> None:
        """Inicia o coordenador."""
        tarefas = {
            asyncio.create_task(self.broker.manter_conexao()),
            asyncio.create_task(self.manter_sinal_de_vida()),
            asyncio.create_task(self.registrar_visao()),
        }
        try:
            concluidas, _ = await asyncio.wait(tarefas, return_when=asyncio.FIRST_EXCEPTION)
            for tarefa in concluidas:
                tarefa.result()
        finally:
            await self.anunciar_offline()
            tarefas |= set(self._disputas.values())  # Janelas de disputa/handoff em andamento
            for tarefa in tarefas:
                tarefa.cancel()
            await asyncio.gather(*tarefas, return_exceptions=True)

        
