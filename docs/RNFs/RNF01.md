# **RNF01 \- Plataforma do coletor**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF01 |
| Nome | Plataforma do coletor |
| Categoria | Portabilidade |
| Descrição | O coletor deve ser executado em Raspberry Pi com Linux, utilizando a pilha BlueZ acessada via D-Bus, e ligado à rede local por cabo. |

**Metas:**

* O coletor se conecta à rede por Ethernet, com o Wi-Fi do Raspberry desligado.

**Justificativa:** no Raspberry Pi, o Wi-Fi e o Bluetooth dividem o mesmo chip e a mesma antena de 2,4 GHz. Com o Wi-Fi em uso, a conexão BLE e o RSSI pioram, o que afeta a decisão de *handoff* e as metas do RNF05.
