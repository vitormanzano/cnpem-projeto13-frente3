# **RNF06 \- Disponibilidade e recuperação de falhas**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF06 |
| Nome | Disponibilidade e recuperação de falhas |
| Categoria | Disponibilidade |
| Descrição | Coletores, coordenador e *broker* devem se manter disponíveis na janela de operação do laboratório e se recuperar de falhas sem intervenção manual. |

**Metas:**

* *Broker* e coordenador disponíveis em ao menos 99,5% da janela de operação (segunda a sexta, das 8h às 18h): no máximo cerca de 1 h de parada não planejada por mês. Manutenção planejada fora da janela não conta. A medição é feita pelas falhas no sinal de vida do coordenador.
* Com a pulseira ao alcance de algum coletor, ao menos 99% dos segundos da sessão têm leitura de frequência cardíaca publicada, sem contar os *handoffs* (RNF04).
* Tempos de recuperação, sem intervenção manual:
  * processo do coletor cai: presença *online* de novo em até 30 s;
  * Raspberry perde energia: coletor *online* em até 2 min depois que a energia volta;
  * energia volta em todos os nós ao mesmo tempo: pulseiras ao alcance conectadas em até 3 min;
  * *broker* reinicia: clientes reconectados em até 15 s e frequência cardíaca de volta ao tópico em até 30 s;
  * coordenador reinicia: posse republicada em até 10 s (RF19);
  * conexão BLE cai com a pulseira ao alcance: reconectada em até 15 s em pelo menos 90% dos casos (RF05).
* Teste de resistência: 8 h com falhas provocadas e nenhuma intervenção manual; depois, 72 h sem falhas provocadas.

**Justificativa:** com um único coordenador e um único *broker*, os dois são ponto único de falha. Chegar a 99,9% (cerca de 13 min por mês) exigiria uma máquina reserva pronta. Já 99,5% é alcançável com `restart: unless-stopped`, nobreak na máquina central e cartões SD de alta durabilidade, que resistem melhor a quedas de energia. "Sem intervenção" só pode ser verificado com prazo. No NB4, a intervenção também custa caro: reiniciar um Raspberry exige entrar na área de contenção com EPI e seguir o protocolo de acesso.
