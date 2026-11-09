# **RNF04 \- Tempo de *handoff***

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF04 |
| Nome | Tempo de *handoff* |
| Categoria | Desempenho |
| Descrição | A transferência da pulseira entre coletores deve ocorrer rapidamente, minimizando o período sem monitoramento. A lacuna de dados de um *handoff*, da desconexão da origem até o vencedor publicar o estado conectado, deve ficar em até 20 s em 95% dos *handoffs*; quando passar disso, o coordenador registra um aviso (RF06). |

**Metas:**

* Lacuna de dados por *handoff*: p50 ≤ 10 s e p95 ≤ 20 s, medidos em pelo menos 30 *handoffs* no percurso de teste.
* Contando os *handoffs*, ao menos 95% dos segundos do percurso de teste têm leitura de frequência cardíaca publicada.
* Com 4 *handoffs* simultâneos (um por pulseira), cada um cumpre as mesmas metas.

**Justificativa:** pelos prazos dos RFs, o caso comum leva de 6 a 9 s: liberação, janela de 3 s e conexão. Os piores caminhos previstos levam 13 s (devolução à origem: 3 + 5 + 5 s), 14 s (janela estendida: 9 + 5 s) e 19 s (os dois juntos). O limite de 20 s cobre o pior caminho previsto. Como teto absoluto, ele seria violado por qualquer medição fora do comum; como percentil, mede o comportamento real. Os 95% de cobertura equivalem a um *handoff* de 15 s a cada 5 min. Para a meta ser alcançável, o tempo limite de conexão BLE (`CONNECT_TIMEOUT`, hoje 20 s) não pode passar do prazo de conexão do RF14 (5 s).
