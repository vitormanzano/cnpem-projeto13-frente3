# Linguagem ubíqua
É uma linguagem comum e rigorosa compartilhada entre todas as pessoas envolvidas no projeto. Tendo benefícios como:
- Elimina ambiguidade: Todos usam exatamente as mesmas palavras para descrever os conceitos do sistema.
- Conecta o negócio com o código: Os termos do vocabulário aparecem no sistema.

## Termos (Português)
- Pesquisadores: Pessoas que utilizarão a pulseira.
- Operador: Pessoa que instala e configura os coletores e o coordenador nos Raspberry Pi e acompanha o painel (RF20).
- Pulseira: Pulseira Polar que será usada pelos pesquisadores do laboratório para monitorar sinais vitais.
- Coletor BLE: Dispositivo Raspberry PI que será utilizado para se conectar com a pulseira.
- Coletor ativo: Coletor que detém a posse da pulseira e está conectado a ela.
- Coletor inativo: Coletor sem a posse da pulseira. Enquanto a posse estiver liberada, detecta os anúncios dela e publica o RSSI; a pulseira conectada não anuncia.
- Coordenador: Dispositivo Raspberry PI, sem conexão BLE com as pulseiras, que acompanha os coletores pelo broker e decide a posse e o handoff.
- Posse: Responsabilidade de um único coletor sobre uma pulseira, concedida e liberada pelo coordenador.
- Pulseira órfã: Pulseira cuja posse está em nome de um coletor indisponível (presença offline ou sem sinal de vida). O coordenador libera a posse para que os coletores que a detectam disputem uma nova (RF14).
- Disputa: Janela em que o coordenador reúne o RSSI publicado pelos coletores para uma pulseira com a posse liberada e concede a posse ao melhor.
- Favorito: Último coletor ativo de uma pulseira. Numa disputa, só perde a pulseira para quem for melhor pela margem configurada.
- Consumidor: Qualquer sistema que recebe pelo broker os dados das pulseiras, sem precisar saber qual coletor as atende. Hoje, o painel (RF20).
- Broker: Servidor MQTT (HiveMQ) que recebe as mensagens dos coletores e do coordenador e as distribui.
- Sinal de vida: Mensagem periódica que coletores e coordenador publicam para indicar que continuam ativos (RF12, RF19).
- Handoff: Troca do coletor ativo de uma pulseira: a origem libera e os coletores disputam a posse. A origem, como favorita, pode vencer e reconectar.
- RSSI: Intensidade do sinal entre um coletor e uma pulseira, em dBm.
- Estado da conexão: conectado | desconectado | nao-encontrado, classificados pelo coletor (RF08); conexao-fraca, classificado pelo coordenador a partir do RSSI publicado (RF19).
