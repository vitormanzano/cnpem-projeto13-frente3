# **RNF11 \- Manutenibilidade do código**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF11 |
| Nome | Manutenibilidade do código |
| Categoria | Manutenibilidade |
| Descrição | O código deve ser organizado para facilitar evolução e testes. |

**Metas:**

* As regras de disputa, posse e *handoff* e o formato das mensagens (RF15) têm testes automatizados que rodam com um único comando, sem pulseira e sem *broker*, em até 1 min.
* O coletor pode rodar com o rádio simulado, para testar coordenador, painel e escala sem hardware (RNF12).
* Só o módulo `bluetooth/` importa o `bleak`: trocar de pulseira não exige mudar o coordenador, o MQTT nem os consumidores.

**Justificativa:** os testes com a pulseira dependem do laboratório e são lentos, então a lógica de decisão precisa ser testada sem hardware. A proposta do projeto pede uma fonte de dados simulada e que os consumidores continuem funcionando se a pulseira for trocada. Hoje o `bleak` já está restrito a `bluetooth/`, e a meta garante que continue assim.
