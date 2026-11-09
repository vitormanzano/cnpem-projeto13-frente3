# **RNF12 \- Escalabilidade**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF12 |
| Nome | Escalabilidade |
| Categoria | Escalabilidade |
| Descrição | O sistema deve atender à escala prevista e permitir adicionar coletores para cobrir novas áreas do laboratório, sem alteração de código. |

**Metas:**

* Escala nominal: 4 pulseiras, 5 coletores e até 5 consumidores simultâneos, cumprindo os RNF04 e RNF05.
* Folga: com coletores e pulseiras simulados (RNF11), as metas do RNF05 continuam valendo com o dobro da escala (8 pulseiras e 10 coletores). O *broker* sustenta 10 vezes o tráfego nominal (cerca de 250 mensagens/s) sem perder mensagens QoS 1.
* Adicionar um coletor leva até 30 min e exige só o Raspberry preparado e o `.env`, sem reiniciar o coordenador nem os outros coletores.
* Com mais pulseiras que coletores numa área, a pulseira que ficar sem coleta aparece no painel (`nao-encontrado` ou posse liberada) em até 15 s.
* Coletor, coordenador e *broker* não gravam dados fisiológicos; o disco de cada nó só cresce com os *logs* (RNF07).

**Justificativa:** o tráfego nominal é de 15 a 25 mensagens de cerca de 250 B por segundo (cerca de 6 KB/s). O limite real está no rádio e no coordenador, que roda num único processo, e não no *broker*. O coordenador já reconhece coletores novos pela presença (RF13). Como cada coletor atende uma pulseira por vez, ter mais pulseiras que coletores numa área é um limite do próprio desenho e precisa aparecer no painel, em vez de ser descoberto em campo. Para quem for guardar o histórico, a referência é de cerca de 7 MB de frequência cardíaca por pulseira a cada 8 h: aproximadamente 0,6 GB por mês para 4 pulseiras, mais cerca de 1 GB se o RSSI também for gravado.
