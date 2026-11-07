# Linguagem ubíqua
É uma linguagem comum e rigorosa compartilhada entre todas as pessoas envolvidas no projeto. Tendo benefícios como:
- Elimina ambiguidade: Todos usam exatamente as mesmas palavras para descrever os conceitos do sistema.
- Conecta o negócio com o código: Os termos do vocabulário aparecem no sistema.

## Termos (Português)
- Pesquisadores: Pessoas que utilizarão a pulseira.
- Pulseira: Pulseira Polar que será usada pelos pesquisadores do laboratório para monitorar sinais vitais.
- Coletor BLE: Dispositivo Raspberry PI que será utilizado para se conectar com a pulseira.
- Coletor ativo: Coletor que detém a posse da pulseira e está conectado a ela.
- Coletor inativo: Coletor sem a posse da pulseira, que apenas detecta os anúncios dela e publica o RSSI.
- Coordenador: Dispositivo Raspberry PI, sem conexão BLE com as pulseiras, que acompanha os coletores pelo broker e decide a posse e o handoff.
- Posse: Responsabilidade de um único coletor sobre uma pulseira, concedida e liberada pelo coordenador.
- Pulseira órfã: Pulseira cuja posse está em nome de um coletor indisponível (presença offline). O coordenador libera a posse para que os coletores que a detectam disputem uma nova (RF14).
- Consumidor: Sistema que recebe pelo broker os dados fisiológicos das pulseiras.
- Broker: Servidor MQTT (HiveMQ) que recebe as mensagens dos coletores e do coordenador e as distribui.
- Handoff: Transferência da conexão de uma pulseira de um coletor para outro.
- RSSI: Intensidade do sinal entre um coletor e uma pulseira, em dBm.
- Estado da conexão: conectado | desconectado | conexao-fraca | nao-encontrado.
