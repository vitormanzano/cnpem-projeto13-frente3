# **RNF08 \- Segurança da comunicação com o *broker***

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF08 |
| Nome | Segurança da comunicação com o *broker* |
| Categoria | Segurança |
| Descrição | A comunicação de coletores, coordenador e consumidores com o *broker* deve ser protegida contra acesso não autorizado: na fase atual, pelo isolamento da rede; antes de coletar dados de pesquisadores fora da equipe ou de publicar o projeto como *open-source*, também por autenticação, autorização e criptografia no *broker*. |

**Metas da fase atual:**

* O *broker* só é acessível a partir da rede do laboratório (VLAN própria, sem redirecionamento de porta e sem saída para a internet). Uma varredura de portas feita de outra rede não encontra a porta 1883.
* As mensagens publicadas no *broker* identificam só a pulseira (endereço MAC) e o coletor, nunca o pesquisador. O vínculo entre pulseira e pesquisador fica fora do *broker*.

**Metas da fase seguinte:**

* O *broker* recusa conexões anônimas. Cada nó tem credencial própria, guardada só no `.env`, e a distribuição não traz credencial padrão.
* Cada papel publica e assina apenas os tópicos que o RF15 atribui a ele, e os consumidores só assinam. Teste: um coletor que tenta publicar em `pulseiras/{endereco}/posse` é recusado.
* A comunicação usa TLS 1.2 ou superior na porta 8883, com certificados de uma autoridade certificadora local, e a porta 1883 fica desligada.

**Justificativa:** a decisão atual é manter o *broker* sem autenticação; por isso, nesta fase, a proteção vem do isolamento da rede. Hoje, `ports: "1883:1883"` expõe o *broker* em todas as interfaces da máquina central. A frequência cardíaca de uma pessoa identificável é dado pessoal sensível (LGPD, art. 5º, II), e a pseudonimização entra desde já porque mudar tópicos e campos depois quebra os consumidores (RF15). Na fase seguinte, o pior risco a cobrir é um cliente qualquer publicar uma posse ou um *handoff* e derrubar a coleta. O HiveMQ CE aceita qualquer cliente por padrão: autenticação e controle por tópico exigem a extensão File RBAC ou a troca pelo Mosquitto. Sem internet não há Let's Encrypt, daí a autoridade certificadora local. Entre a pulseira e o coletor, a frequência cardíaca trafega sem pareamento; o risco é aceito porque o acesso físico ao NB4 é controlado.
