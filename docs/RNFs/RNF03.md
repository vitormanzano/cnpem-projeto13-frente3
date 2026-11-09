# **RNF03 \- Operação sem internet**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF03 |
| Nome | Operação sem internet |
| Categoria | Disponibilidade |
| Descrição | Coletores, coordenador, *broker* e painel devem operar sem acesso à internet, dependendo apenas da rede local do laboratório. |

**Metas:**

* Com a saída para a internet bloqueada, todos os RFs funcionam (roteiro de teste executado com o *uplink* desligado).
* Em execução, nenhum componente faz requisições para fora da rede local. O painel serve as próprias bibliotecas e fontes.
* Os relógios dos nós são sincronizados por um servidor NTP local (chrony na máquina central), com diferença de até 10 ms entre eles. O coletor só começa a publicar depois de sincronizar.

**Justificativa:** o RF20 exige o painel sem internet, e uma biblioteca de gráficos carregada de um CDN deixa a página em branco no laboratório, sem aviso. O Raspberry Pi até o modelo 4 não tem relógio de hardware, e o Pi 5 só tem com bateria. Sem internet, o Raspberry reinicia com a hora atrasada. Com a hora atrasada, o coordenador descarta as mensagens do coletor por parecerem antigas (RF13), a deduplicação por *timestamp* falha (RF17) e não é possível medir a latência do RNF05.
