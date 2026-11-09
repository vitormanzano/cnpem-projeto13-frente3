# **RNF05 \- Latência de detecção e publicação**

| Campo | Descrição |
| :---- | :---- |
| Identificador | RNF05 |
| Nome | Latência de detecção e publicação |
| Categoria | Desempenho |
| Descrição | Mudanças na conexão devem ser detectadas e publicadas, e as leituras de frequência cardíaca devem chegar aos consumidores, dentro dos limites abaixo. |

**Metas:**

* Mudança de estado percebida pelo coletor: publicada em até 1 s (p95).
* Queda da conexão BLE: percebida pelo coletor em até o tempo de supervisão negociado com a pulseira mais 1 s. O valor negociado deve ser registrado em *log*.
* Conexão fraca: da 3ª leitura abaixo do limiar até a publicação do comando de *handoff*, até 1 s (p95). Com uma leitura de RSSI por segundo, o enfraquecimento vira comando em até 4 s.
* Coletor que cai sem avisar: considerado indisponível e com a posse liberada em até 15 s.
* Frequência cardíaca, do *timestamp* do coletor até a entrega ao consumidor: p50 ≤ 50 ms, p95 ≤ 200 ms e p99 ≤ 500 ms.
* Painel: primeiro dado de cada pulseira na tela em até 2 s após abrir.

**Justificativa:** o painel atualiza a cada 1 s e mostra "sem leitura" após 5 s (RF20). Com p99 abaixo de 500 ms, a latência nunca faz o painel perder uma atualização. Os demais limites seguem os prazos que os RFs já definem (RF13, RF14, RF19). Os 15 s vêm do *keepalive* de 10 s usado no código, porque o *broker* publica a última vontade após 1,5 vez o *keepalive*. Se o *keepalive* passar a 5 s, como pede o RF11, esse limite pode cair para 8 s. Para medir a latência, os relógios precisam estar sincronizados (RNF03).
