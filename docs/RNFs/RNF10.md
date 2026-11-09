# **RNF10 \- Observabilidade**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF10 |
| Nome | Observabilidade |
| Categoria | Manutenibilidade |
| Descrição | O comportamento do coletor e do coordenador deve ser rastreável por *logs*. |

**Metas:**

* Cada linha de *log* traz horário em UTC (ISO 8601), nível, componente e identificador do nó.
* Todo evento que um RF manda registrar aparece no *log*: mudanças de estado, quedas e reconexões, disputas, concessões, liberações e *handoffs*, com o identificador.
* As metas dos RNF04, RNF05 e RNF06 podem ser calculadas só a partir dos *logs*, sem instrumentação extra.
* Os *logs* não contêm valores de bpm ou RR nem o nome do pesquisador.

**Justificativa:** coletores e coordenador não guardam histórico (RF20). Por isso, os *logs* são o único registro do que aconteceu e a fonte para medir as metas de desempenho e disponibilidade. Manter os valores fisiológicos fora dos *logs* evita que dados de saúde fiquem gravados em disco.
