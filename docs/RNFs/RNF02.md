# **RNF02 \- Implantação em container**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF02 |
| Nome | Implantação em container |
| Categoria | Portabilidade |
| Descrição | O coletor, o coordenador e o *broker* devem ser implantados via Docker e Docker Compose, sem depender de internet na instalação. |

**Metas:**

* Cada máquina sobe com `docker compose up -d`, mudando só o `.env` (o `COMPOSE_PROFILES` escolhe o papel).
* Um coletor pode ser reinstalado do zero, sem internet, em até 30 min, a partir de uma imagem do cartão SD e das imagens Docker salvas (`docker save`) ou de um registro local.
* As máquinas usam Docker Engine, e não Docker Desktop.

**Justificativa:** o `docker-compose.yml` já define os três serviços, e a descrição anterior cobria só o coletor. Sem internet no laboratório (RNF03), não dá para rodar `docker pull` nem `pip install` na hora de trocar um Raspberry com defeito. O Docker Desktop pode exigir assinatura paga em organizações com mais de 250 funcionários; o Engine é livre, e o `start.sh` já o instala.
