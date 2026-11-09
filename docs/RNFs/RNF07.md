# **RNF07 \- Consumo de recursos**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF07 |
| Nome | Consumo de recursos |
| Categoria | Eficiência |
| Descrição | O coletor deve ser leve o suficiente para rodar em Raspberry Pi sem comprometer outras funções, inclusive o armazenamento. |

**Metas** (medidas com `docker stats` durante 24 h, no modelo de Raspberry Pi adotado):

* CPU: média de até 15% de um núcleo e pico de até 50%.
* Memória do container: até 150 MB, sem crescer mais de 10% em 24 h.
* *Logs*: rotação configurada no `docker-compose.yml`, com até 100 MB por nó.

**Justificativa:** as metas transformam "sem comprometer outras funções" em algo verificável. O limite de *logs* já faz falta: o `docker-compose.yml` não tem a opção `logging:`, então o *log* do Docker cresce até encher o cartão SD. Acompanhar o crescimento de memória por 24 h revela vazamentos que um teste curto não mostra.
