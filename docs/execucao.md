# Execução e testes do gateway

Como rodar o sistema em Linux de verdade e testar cada fluxo, da bancada ao percurso no laboratório.

Os testes vão em cinco níveis, do mais barato ao mais caro. Só passe ao próximo com o anterior sem falhas: um problema de permissão do `btmgmt` encontrado no campo custa uma tarde, e na bancada custa um minuto.

| Nível | O que testa | Precisa de | Seção |
|---|---|---|---|
| 1. Testes automatizados | Regras, ordem das mensagens e prazos, com dublês do BLE e do broker | Nada | [3](#3-nível-1-testes-sem-hardware) |
| 2. Diagnóstico por RF | Cada RF isolado, com a pulseira e o broker reais (`main.py`) | 1 máquina com Bluetooth, 1 pulseira, broker | [4](#4-nível-2-diagnóstico-com-hardware-mainpy) |
| 3. Sistema com 1 coletor | Posse, quedas, reinícios e painel com o `app.py` | Central + 1 coletor (pode ser a mesma máquina) | [6](#6-nível-3-sistema-completo-com-um-coletor) |
| 4. Disputa e handoff | Disputa, favorito, pulseira órfã e handoff | Central + 2 coletores em máquinas diferentes | [7](#7-nível-4-dois-coletores-disputa-e-handoff) |
| 5. Campo | Percurso real, calibração e metas do RNF04 | Central + coletores instalados no laboratório | [8](#8-nível-5-teste-em-campo) |

Todos os comandos rodam na **raiz do projeto**.

---

## 1. Papéis das máquinas

| Máquina | O que roda | Bluetooth |
|---|---|---|
| Central | Broker (HiveMQ), coordenador e painel | Não usa |
| Coletor | `app.py` com `PAPEL=coletor` | Usa o adaptador `hci0` |

- **Um coletor por máquina.** O RSSI da conexão vem de `btmgmt --index 0` e o bleak usa o adaptador padrão. Dois coletores na mesma máquina (com `COLETOR_ID` diferentes) dividem o mesmo rádio e medem o mesmo RSSI: servem para testar mensagens, não disputa nem handoff.
- Para os níveis 4 e 5 são necessárias pelo menos **duas máquinas com Bluetooth**. Seu PC pode ser central e coletor 1 ao mesmo tempo, com uma Raspberry como coletor 2.
- Na versão atual, cada coletor atende **uma** pulseira (`TARGET_ADDRESS`).

---

## 2. Preparar as máquinas

### 2.1 Bluetooth (em cada coletor)

```bash
# Raspberry Pi OS / Debian / Ubuntu
sudo apt-get install -y bluez
# Arch
sudo pacman -S bluez bluez-utils

sudo systemctl enable --now bluetooth
sudo rfkill unblock bluetooth
bluetoothctl power on
bluetoothctl show              # deve mostrar "Powered: yes"
sudo btmgmt --index 0 info     # o adaptador usado precisa ser o hci0
```

### 2.2 Permissão do `btmgmt` (só na execução nativa)

O RSSI da conexão ativa (RF04) é lido com `btmgmt conn-info`, que exige root ou `CAP_NET_ADMIN`. Sem a permissão, o coletor conecta e recebe a frequência cardíaca, mas o RSSI sai como `rssi-indisponivel`. Aí o coordenador nunca classifica `conexao-fraca` e **o handoff não acontece**.

```bash
sudo setcap cap_net_admin+eip "$(command -v btmgmt)"
getcap "$(command -v btmgmt)"   # deve mostrar cap_net_admin=eip
```

- No Docker não é preciso: o serviço `coletor` é `privileged` e roda como root.
- A permissão se perde quando o pacote do BlueZ é atualizado. Se o `rssi-indisponivel` voltar depois de um `apt upgrade` ou `pacman -Syu`, rode o `setcap` de novo.

### 2.3 Docker

| Sistema | Instalação |
|---|---|
| Ubuntu | `./start.sh` |
| Raspberry Pi OS | `curl -fsSL https://get.docker.com \| sh` e `sudo usermod -aG docker $USER` (o `start.sh` usa o repositório do Ubuntu e não serve) |
| Arch | `sudo pacman -S docker docker-compose docker-buildx` e `sudo usermod -aG docker $USER` |

Em seguida, nos três sistemas:

```bash
sudo systemctl enable --now docker   # "enable": com restart: unless-stopped, os serviços voltam sozinhos quando a máquina religa
```

Saia da sessão e entre de novo para o grupo `docker` valer.

### 2.4 Hostname: o id do coletor

O id do coletor é o hostname (`uname -n`). Dois coletores com o mesmo id derrubam a sessão um do outro no broker, em laço.

```bash
sudo hostnamectl set-hostname coletor-sala-1
```

Use nomes que digam onde o coletor está instalado: no campo, o log `Posse concedida | Coletor: coletor-sala-2` já diz onde a pulseira está.

### 2.5 Relógios sincronizados (sem internet)

O `timestamp` de cada mensagem vem do relógio de quem publica, e dois pontos dependem dele:

- o coordenador ignora a mensagem de um coletor com timestamp anterior ao da última recebida desse coletor (RF13);
- o painel descarta a frequência cardíaca com timestamp anterior ao da última leitura da pulseira.

A Raspberry não tem relógio com bateria. Sem internet, ao religar ela volta no tempo. Aí o coordenador passa a ignorar esse coletor até o relógio alcançar a última mensagem, e num handoff para um coletor atrasado o painel descarta a frequência cardíaca dele. Por isso a central serve a hora para a rede local.

Na central (chrony):

```bash
sudo apt-get install -y chrony      # ou: sudo pacman -S chrony
# Em /etc/chrony/chrony.conf (Debian/Ubuntu) ou /etc/chrony.conf (Arch), acrescente:
#   allow 192.168.0.0/24      # a rede do laboratório
#   local stratum 10          # serve a própria hora mesmo sem internet
sudo systemctl restart chrony       # no Arch: chronyd
```

Em cada coletor (systemd-timesyncd, o padrão do Raspberry Pi OS):

```bash
# Em /etc/systemd/timesyncd.conf, na seção [Time]:
#   NTP=192.168.0.10          # IP da central
sudo systemctl restart systemd-timesyncd
timedatectl timesync-status       # "Server: 192.168.0.10"
```

Para conferir, rode `date -u +%T.%N` em todas as máquinas ao mesmo tempo. A diferença deve ficar abaixo de 1 s.

### 2.6 Rede

A central precisa aceitar conexões nas portas `1883/tcp` (broker), `8765/tcp` (painel) e `123/udp` (hora). Se houver firewall (ufw, firewalld), libere as três. De um coletor, confira:

```bash
nc -zv 192.168.0.10 1883
```

### 2.7 `.env` de cada máquina

```bash
cp .env.example .env
```

O repositório e o `docker-compose.yml` são os mesmos em todas as máquinas. Muda só o `.env`:

| Variável | Central | Coletor |
|---|---|---|
| `COMPOSE_PROFILES` | `central` | `coletor` |
| `BROKER_HOST` | (o compose usa `hivemq`) | IP da central |
| `TARGET_ADDRESS` | MAC da pulseira | MAC da pulseira |
| `PULSEIRAS` | MAC da pulseira (vírgula para várias) | não usa |
| `COORDENADOR_ID` | **igual em todas** | **igual em todas** |
| `PRAZO_CONEXAO` | **igual em todas** | **igual em todas** |

- Para descobrir o MAC da pulseira, rode o `teste_rf01` (seção 4).
- Os coletores só aceitam posse e handoff com origem `COORDENADOR_ID`. Um valor diferente aparece no coletor como `Mensagem ignorada | ... não é o coordenador`.
- Para rodar tudo numa máquina só: `COMPOSE_PROFILES=central,coletor` e `BROKER_HOST=localhost`. O coletor usa a rede do host, e o coordenador e o painel recebem `BROKER_HOST=hivemq` do próprio compose.

### 2.8 Python para a execução nativa

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

A imagem Docker usa Python 3.11. Para rodar nativo, use 3.11 ou mais novo.

### 2.9 Pulseira

- Ligada, no braço e no modo que transmite a frequência cardíaca por Bluetooth.
- **Sem conexão com o celular.** Com o Polar Flow ou o nRF Connect conectado, ela some para os coletores. Desligue o Bluetooth do celular durante os testes.

---

## 3. Nível 1: testes sem hardware

Os testes de `tests/` rodam sem pulseira, sem broker e sem Bluetooth. Os dublês fazem o papel do bleak e do aiomqtt, e os relógios falsos verificam os prazos sem esperar. Rode-os antes de cada ida à bancada e depois de cada mudança no código.

```bash
source .venv/bin/activate
for f in tests/test_*.py; do python "$f" || { echo "FALHOU: $f"; break; }; done
```

Cada teste aprovado imprime `ok  test_...`. Com o pytest instalado, `python -m pytest tests -q` faz o mesmo.

| Arquivo | Cobre |
|---|---|
| `test_reconexao.py` | RF05: reconexão, limite de tentativas, desconexão intencional |
| `test_rssi.py` | RF04: leitura do `btmgmt`, falhas seguidas |
| `test_estado_conexao.py` | RF08: transições de estado |
| `test_desconexao.py` | RF10 e RF05: ordem do encerramento, liberação após a reconexão esgotada |
| `test_varredura.py` | RF18: mediana, amostras, `nao-encontrado`, varredura conforme a posse |
| `test_mensagens.py` | RF15: envelope, versão, campos obrigatórios, descarte |
| `test_broker.py` | RF11: reconexão, reassinatura, prazo sem broker |
| `test_frequencia_cardiaca.py` | RF17: só com a posse, buffer sem broker, ordem |
| `test_coordenador.py` | RF13, RF14, RF19, RF06: visão, disputa, favorito, órfã, handoff, carência, reinício |
| `test_painel.py` | RF20: classificação do sinal, "sem leitura", histórico |

Os caminhos raros de provocar com hardware (handoff cancelado, devolução à origem, posse que chega durante a janela, reinício do coordenador no meio de um handoff) estão em `test_coordenador.py`. No campo, basta reconhecê-los no log quando aparecerem.

### Painel sem hardware (modo simulado)

O modo simulado põe dois coletores e um coordenador falsos para publicar uma pulseira simulada, com handoff, queda e trecho sem frequência cardíaca. Não rode o coordenador real ao mesmo tempo.

```bash
docker compose up -d hivemq
PAINEL_SIMULADO=1 PAPEL=painel BROKER_HOST=localhost python src/gateway/app.py
# abra http://localhost:8765
```

---

## 4. Nível 2: diagnóstico com hardware (`main.py`)

O `main.py` tem um `teste_rfXX()` por RF. Cada critério de aceitação aparece no log como `[OK]` ou `[FALHA]`, e no fim sai um `Resumo`.

### Como rodar

**Opção A: editar o `main()`.** Descomente **um** teste e rode:

```bash
python src/gateway/main.py
```

**Opção B: sem editar o arquivo.** Mais prática na Raspberry. Dá para passar argumentos ao teste; o `Resumo` final não sai, mas cada `[OK]`/`[FALHA]` aparece.

```bash
PYTHONPATH=src/gateway python -c "import asyncio, main; asyncio.run(main.teste_rf04(duracao=60))"
```

**No Docker**, use os mesmos comandos depois de `docker compose run --rm coletor`, por exemplo:

```bash
docker compose run --rm coletor python -c "import asyncio, main; asyncio.run(main.teste_rf01())"
```

Cuidados:

- O `main.py` usa o hostname como client id. **Não rode junto com o coletor** (`app.py`) da mesma máquina, senão o broker derruba uma das sessões. Pare o coletor antes ou use `COLETOR_ID=diag-$(uname -n)` na frente do comando.
- Os testes com broker (RF11, RF12, RF09, RF17) precisam do HiveMQ de pé: `docker compose up -d hivemq` na central.
- `docker stop cnpem_hivemq` e `docker start cnpem_hivemq` rodam **na central**.

### Ordem e o que observar

| # | Teste | Precisa | Ação manual | O que observar |
|---|---|---|---|---|
| 1 | `teste_rf15()` | nada | — | Formato, QoS e tópicos |
| 2 | `teste_rf01()` | Bluetooth | `bluetoothctl power off` antes, para ver a mensagem de Bluetooth desligado | Lista com o MAC da pulseira: copie-o para `TARGET_ADDRESS` |
| 3 | `teste_rf02()` | pulseira | — | Para no primeiro anúncio; endereço inexistente gera `pulseira-nao-encontrada` |
| 4 | `teste_rf03()` | pulseira | Rode 5 vezes | **Anote o `conectou em X s`**: define o `PRAZO_CONEXAO` (seção 8.2) |
| 5 | `teste_rf07()` | pulseira | — | Serviços com UUID e descrição |
| 6 | `teste_rf04()` | pulseira + `btmgmt` com permissão | Afaste a pulseira durante o teste | RSSI a cada `RSSI_INTERVALO`, caindo com a distância; FC continua chegando |
| 7 | `teste_rf16()` | pulseira no braço | Tire do braço por mais de 5 s e recoloque | `Nenhuma leitura de frequência cardíaca há 5s` e depois `retomadas` |
| 8 | `teste_rf08(esperar_queda=True)` | pulseira | Depois de conectar, desligue ou afaste a pulseira | Transições com horário, estado anterior e novo |
| 9 | `teste_rf10()` | pulseira | — | Não reconecta; a pulseira volta a anunciar |
| 10 | `teste_rf05()` | pulseira | Afaste e traga de volta; depois deixe fora por mais de `RECONEXAO_MAX_TENTATIVAS × (CONNECT_TIMEOUT + RECONEXAO_INTERVALO)` (115 s no padrão) | `Queda inesperada`, tentativas, `Reconectada em X s`; por fim a falha informada |
| 11 | `teste_rf05(desconectar_apos=15)` | pulseira | — | Desconexão intencional não reconecta |
| 12 | `teste_rf11(manual=True)` | broker | `docker stop cnpem_hivemq`, espere, `docker start cnpem_hivemq` | `broker-indisponivel` a cada `BROKER_INTERVALO_RECONEXAO`, sem encerrar; reconecta e reassina |
| 13 | `teste_rf12()` | broker | — | `online` retido, sinal de vida, última vontade na queda, `offline` no fim |
| 14 | `teste_rf09()` | pulseira + broker | `docker stop` e `docker start` do broker no meio | `broker-indisponivel` com o RSSI continuando no log |
| 15 | `teste_rf17()` | pulseira no braço + broker | `docker stop` por **menos de 15 s** e `docker start` | Na volta, `Publicando N leitura(s) ... guardada(s)` |

Para ver tudo o que passa no broker num segundo terminal: `espiar("#")`, ou o espião da seção 5.1.

---

## 5. Ferramentas usadas em todos os fluxos

### 5.1 Espião do broker

```bash
docker run --rm -it --network host eclipse-mosquitto:2 \
  mosquitto_sub -h localhost -t '#' -F '%I | %t | retida=%r | %p'
```

- Rode-o na central, ou troque `localhost` pelo IP dela.
- Ele usa só o cliente `mosquitto_sub` da imagem, não o broker Mosquitto. As aspas no `'#'` evitam que o zsh trate o resto da linha como comentário.
- Ao conectar, chegam as mensagens **retidas** (`retida=1`): o último estado de cada coletor e a posse de cada pulseira, inclusive de execuções anteriores.

| Filtro (`-t`) | Mostra |
|---|---|
| `'#'` | Tudo |
| `'coletores/+/presenca'` | Presença dos coletores |
| `'coletores/+/pulseiras/+'` | Estado e RSSI (com `amostras` na varredura) |
| `'pulseiras/+/posse'` | Posse de cada pulseira |
| `'pulseiras/+/#'` | Posse, liberação, handoff e frequência cardíaca |
| `'coordenador/#'` | Presença e sinal de vida do coordenador |

`+` casa exatamente um nível e `#` casa o resto do tópico (só pode ser o último).

### 5.2 Logs

```bash
# Docker
docker compose logs -f --timestamps coordenador
docker compose logs -f --timestamps coletor

# Nativo, guardando em arquivo (logs/*.log já está no .gitignore)
mkdir -p logs
PAPEL=coletor python src/gateway/app.py 2>&1 | tee -a "logs/$(uname -n)-$(date +%F).log"
```

Com `LOG_LEVEL=DEBUG` no coordenador aparecem cada sinal recebido (`RSSI`, `Amostras`, `Válido`), os sinais de vida e a visão da rede a cada 10 s. Use-o na calibração.

### 5.3 Painel

Abra `http://<IP da central>:8765` em qualquer computador da rede.

### 5.4 Limpar mensagens retidas

Uma presença retida de um id antigo aparece como coletor fantasma. Para apagá-la:

```bash
docker run --rm --network host eclipse-mosquitto:2 \
  mosquitto_pub -h localhost -t 'coletores/<id-antigo>/presenca' -r -n
```

Para começar um teste do zero, sem posse, presença retida nem sessão persistente do coordenador:

```bash
docker compose down -v    # -v apaga os volumes do HiveMQ
```

---

## 6. Nível 3: sistema completo com um coletor

### Subir

Escolha uma das montagens:

```bash
# Tudo numa máquina (.env: COMPOSE_PROFILES=central,coletor e BROKER_HOST=localhost)
docker compose up -d --build

# Central no Docker e coletor nativo na mesma máquina
docker compose up -d --build hivemq coordenador painel
PAPEL=coletor python src/gateway/app.py

# Máquinas separadas: em cada uma, com o .env da seção 2.7
docker compose up -d --build
```

Deixe abertos: (1) o log do coordenador, (2) o log do coletor, (3) o espião com `'#'` e (4) o painel.

Em cada fluxo, a coluna "Esperado" traz trechos das linhas de log reais, para buscar com o `grep`.

### F1. Partida e primeira posse (RF12, RF18, RF13, RF14, RF03, RF16, RF17, RF20)

Ação: subir tudo com a pulseira ligada e no braço.

| Onde | Esperado, nesta ordem |
|---|---|
| Coordenador | `Presença: online \| Coordenador`; ~2 s depois, `Posse republicada \| ... Coletor: nenhum \| Motivo: reinício do coordenador` |
| Coletor | `Conectado ao broker`, `Presença: online`, `Pulseira encontrada \| ... RSSI` |
| Espião | `coletores/<host>/pulseiras/<MAC>` com `estado: desconectado`, `rssi` e `amostras` a cada segundo |
| Coordenador | `Presença \| Coletor: <host> \| online`, `Disputa aberta \| ... Janela: 3 s`, `Posse concedida \| ... Coletor: <host> \| Motivo: maior RSSI` |
| Coletor | `Posse recebida`, `Conectando à pulseira`, `Estado: desconectado -> conectado` |
| Espião | `pulseiras/<MAC>/posse` retida com o coletor; `pulseiras/<MAC>/frequencia-cardiaca` a cada ~1 s; estado `conectado` com `rssi` e sem `amostras` |
| Painel | FC, RSSI suavizado e bruto, estado `conectado` e o coletor com a posse |

Se aparecer `Disputa sem vencedor`, os sinais não estão válidos: veja `MINIMO_AMOSTRAS` na seção 8.2.

### F2. Encerramento controlado do coletor (RF10, RF12, RF14)

Ação: `Ctrl+C` no coletor nativo ou `docker compose stop coletor`.

| Onde | Esperado |
|---|---|
| Coletor | `Liberando a pulseira ... Motivo: encerramento`, `Desconectando da pulseira`, `Presença: offline`, `Coletor encerrado.` |
| Espião | Nesta ordem: estado `desconectado` → `pulseiras/<MAC>/liberacao` → presença `offline` |
| Coordenador | `Liberação recebida`, `Posse liberada \| ... Motivo: liberação do coletor <host>`, `Coletor indisponível \| ... presença offline` |
| Painel | "sem leitura" depois de 5 s; posse liberada |

Suba o coletor de novo: ele deve repetir o F1.

### F3. Queda do processo do coletor: última vontade e pulseira órfã (RF12, RF14)

Ação: `docker kill $(docker ps -q --filter name=coletor)` ou `pkill -9 -f src/gateway/app.py`.

| Onde | Esperado |
|---|---|
| Espião | Em até ~1,5 × `PRESENCA_INTERVALO_COLETOR` (~8 s), presença `offline` publicada pelo **broker** |
| Coordenador | `Coletor indisponível \| ... presença offline`, `Posse liberada \| ... Motivo: coletor <host> indisponível` |

Depois, rode `bluetoothctl info <MAC>` naquela máquina. Se mostrar `Connected: yes`, o BlueZ manteve a conexão de um processo que morreu: a pulseira não anuncia e nenhum outro coletor a enxerga. Solte-a com `bluetoothctl disconnect <MAC>` e **registre o achado**, porque no campo isso vira uma pulseira perdida até alguém intervir.

### F4. Queda BLE e reconexão (RF05, RF08)

Ação: afaste a pulseira até cair ou feche-a numa lata de metal, que bloqueia o rádio. Traga-a de volta em menos de um minuto.

| Onde | Esperado |
|---|---|
| Coletor | `Queda inesperada após X s conectada; iniciando reconexão`, `Reconexão: tentativa 1/5`, `Reconectada em X s (tentativa N)`, FC retomada |
| Espião | Estado `desconectado` e depois `conectado`; a posse **não muda** |
| Painel | Indica a interrupção e volta sozinho |

Agora deixe a pulseira fora por mais de 115 s (5 × (20 + 3) no padrão):

| Onde | Esperado |
|---|---|
| Coletor | `reconexão esgotada após 5 tentativa(s)`, `Conexão perdida, devolvendo a posse`; a varredura publica `nao-encontrado` |
| Coordenador | `Liberação recebida`, `Posse liberada` |
| Ao trazer de volta | Nova disputa e nova posse (F1) |

### F5. Pulseira sem contato com a pele (RF16)

Ação: tire a pulseira do braço por 10 s e recoloque. O coletor registra `Nenhuma leitura de frequência cardíaca há 5s` e depois `Leituras de frequência cardíaca retomadas`. O painel mostra "sem leitura" na FC enquanto isso.

### F6. Broker cai (RF09, RF11, RF17)

**a) Queda curta (< `PRAZO_SEM_BROKER`, 15 s).** Rode `docker stop cnpem_hivemq`, espere ~10 s e rode `docker start cnpem_hivemq`.

| Onde | Esperado |
|---|---|
| Coletor | `broker-indisponivel ... nova tentativa em 5s`, `Sem broker \| ... Desconecta em 15s se o broker não voltar`; a pulseira continua conectada e a FC continua no log |
| Na volta | `Conectado ao broker`, `Posse confirmada \| Publicando N leitura(s) ... guardada(s)` |
| Painel | Lacuna curta e retomada sozinho |

**b) Queda longa (> 15 s).** Rode `docker stop cnpem_hivemq`, espere ~30 s e rode `docker start cnpem_hivemq`.

| Onde | Esperado |
|---|---|
| Coletor | Aos 15 s: `Liberando a pulseira ... Motivo: sem broker há 15s` (desconecta sem reconexão automática) |
| Na volta | A posse retida decide: se ainda está em nome do coletor, ele a recebe e reconecta; se não, varre e disputa |

### F7. Coordenador indisponível e reinício (RF19)

**a) Coordenador sai.** Rode `docker compose stop coordenador`.

| Onde | Esperado |
|---|---|
| Coletor | `Coordenador indisponível \| Motivo: presença offline \| Posse e conexão mantidas...` |
| Painel | FC e RSSI continuam: a posse e a conexão não mudam |

Com o coordenador fora, provoque uma queda BLE (F4, volta rápida): o coletor reconecta porque ainda tem a posse.

**b) Coordenador volta.** Rode `docker compose start coordenador`.

| Onde | Esperado |
|---|---|
| Coordenador | `Posse republicada \| ... Coletor: <host> \| Motivo: reinício do coordenador` |
| Coletor | `Coordenador disponível`; a pulseira continua conectada, sem nova disputa |

**c) Liberação durante a ausência (sessão persistente).** Pare o coordenador, encerre o coletor com `Ctrl+C` (a liberação é publicada com o coordenador fora) e suba o coordenador. Ele deve receber a liberação guardada pelo broker e deixar a posse liberada (`Liberação guardada` ou `Posse liberada`), não em nome do coletor que saiu.

**d) Queda sem aviso.** Rode `docker kill $(docker ps -q --filter name=coordenador)`. A última vontade do coordenador vira `Coordenador indisponível` no coletor. Depois suba com `docker compose up -d coordenador`.

### F8. Mensagens indevidas (RF13, RF15, RF19)

Publique uma posse falsa, **sem `-r`**: uma posse retida falsa substituiria a verdadeira.

```bash
docker run --rm --network host eclipse-mosquitto:2 mosquitto_pub -h localhost \
  -t 'pulseiras/<MAC>/posse' \
  -m '{"versao": 2, "tipo": "posse", "origem": "intruso", "timestamp": "2026-11-11T12:00:00+00:00", "conteudo": {"pulseira": "<MAC>", "coletor": "<host>"}}'

docker run --rm --network host eclipse-mosquitto:2 mosquitto_pub -h localhost \
  -t 'coletores/<host>/presenca' -m 'isto nao e json'
```

Esperado: no coletor, `Mensagem ignorada | ... Origem intruso não é o coordenador`; no coordenador, `Posse descartada` e `Mensagem descartada`. Nenhum processo cai.

---

## 7. Nível 4: dois coletores (disputa e handoff)

Montagem: central e dois coletores em **máquinas diferentes** (`coletor-a` e `coletor-b`), com o mesmo `TARGET_ADDRESS`, a alguns metros um do outro. Para ver no espião só o que importa:

```bash
docker run --rm -it --network host eclipse-mosquitto:2 \
  mosquitto_sub -h localhost -t 'pulseiras/+/#' -t 'coletores/+/presenca' -F '%I | %t | %p'
```

### F9. Disputa inicial (RF14, RF18)

Ação: suba os dois coletores com a posse liberada (`docker compose down -v` na central antes, se quiser começar do zero) e deixe a pulseira mais perto de A.

| Onde | Esperado |
|---|---|
| Coordenador | `Disputa aberta`, `Posse concedida \| ... Coletor: coletor-a \| Motivo: maior RSSI \| ... RSSI suavizado [coletor-a: ..., coletor-b: ...]` |
| Coletor B | Para de varrer (a posse está com outro) e não publica mais RSSI dessa pulseira |
| Espião | FC só com `"coletor": "coletor-a"` |

**Confira a premissa do handoff.** Com A conectado, rode `teste_rf01()` em B, ou faça uma varredura com o nRF Connect no celular. A pulseira **não deve aparecer**. Se aparecer, ela anuncia mesmo conectada, e isso muda as premissas do RF06 e do RF18: registre.

### F10. Pulseira órfã com outro coletor por perto (RF14)

Ação: **tire A da tomada** (a queda mais realista, e o rádio para junto). Repita também com `docker kill`, para comparar.

| Onde | Esperado |
|---|---|
| Coordenador | Em ~8 s, `Coletor indisponível \| Coletor: coletor-a`, `Posse liberada \| ... coletor coletor-a indisponível`, `Disputa aberta`, `Posse concedida \| ... coletor-b` |
| Coletor B | Volta a varrer, recebe a posse e conecta |

Meça o tempo entre o corte e o `conectado` de B. Com `docker kill`, se a pulseira ficar presa à conexão de A (F3), B nunca a enxerga e aparece `Disputa sem vencedor`.

### F11. Handoff por movimento (RF06, RF19)

Ação: com a pulseira conectada a A, leve-a devagar até perto de B.

| Onde | Esperado, nesta ordem |
|---|---|
| Coordenador | `Estado: conexao-fraca \| ... Coletor: coletor-a \| RSSI: ... \| 3 leituras seguidas < -78 dBm` |
| Coordenador | `Handoff iniciado \| Id: <id> \| ... Origem: coletor-a` |
| Coletor A | `Liberando a pulseira ... Motivo: handoff \| Handoff: <id>`, volta a varrer |
| Coordenador | `Liberação recebida \| ... Handoff: <id>`, `Posse liberada \| Handoff: <id>`, `Disputa aberta \| Handoff: <id> \| ... Favorito: coletor-a` |
| Coordenador | `Posse concedida \| Handoff: <id> \| ... Coletor: coletor-b \| Motivo: maior RSSI` |
| Coordenador | `Handoff concluído \| Id: <id> \| Origem: coletor-a \| Vencedor: coletor-b \| Conectado: coletor-b \| RSSI suavizado [...] \| Lacuna: X s` |
| Espião | O campo `coletor` da FC passa de A para B **uma vez**, com os timestamps sempre crescentes |

Variação **origem favorita**: comece o handoff com a pulseira entre os dois, com B menos de 5 dB melhor. A vence como favorita, e em seguida aparecem `Carência de handoff ... 30 s` e, se o sinal continuar fraco, `Handoff adiado`.

### F12. Handoff forçado na bancada (RF06, RNF04)

Para gerar muitos handoffs sem andar, suba o limiar **só na central**, acima do RSSI que a conexão tem na bancada:

```bash
# .env da central
LIMIAR_CONEXAO_FRACA=-40
```

```bash
docker compose up -d coordenador   # recria o coordenador com o .env novo
```

Toda conexão vira `conexao-fraca` depois de 3 leituras, e os handoffs se sucedem: a carência só atrasa quando a origem vence. É o jeito de juntar dezenas de handoffs para a análise da seção 8.5 antes do campo, e de verificar que a pulseira nunca fica com dois coletores. **Volte o limiar ao valor normal** depois do teste. O painel também lê esse limiar, então ele não muda enquanto você não recriar o painel.

---

## 8. Nível 5: teste em campo

### 8.1 Instalação

1. Desenhe a planta com a posição de cada coletor e o hostname de cada um.
2. Em cada máquina: `.env` (seção 2.7), relógio sincronizado (2.5) e `docker compose up -d --build`.
3. Confira na central:
   - log do coordenador: `Presença | Coletor: <host> | online` para cada coletor;
   - painel aberto e com a pulseira;
   - `date -u +%T` igual em todas as máquinas.
4. Anote a versão testada: `git rev-parse --short HEAD`.

### 8.2 Calibração (antes do percurso)

Os padrões dos parâmetros foram escolhidos sem medição. Ajuste-os nesta ordem, com o coordenador em `LOG_LEVEL=DEBUG`:

| Parâmetro | Onde | Como medir | Como ajustar |
|---|---|---|---|
| `MINIMO_AMOSTRAS` (5) | central | Pare o coletor ativo (posse liberada) e veja `amostras` no espião `'coletores/+/pulseiras/+'`, ou `Amostras: N \| Válido:` no log do coordenador | Um pouco abaixo do valor típico. Acima dele, todos os sinais ficam inválidos e vem `Disputa sem vencedor` |
| `PRAZO_CONEXAO` (5 s) | **central e coletores** | `conectou em X s` do `teste_rf03` em cada coletor, ou o intervalo entre `Conectando à pulseira` e `-> conectado` no log | Acima do maior tempo medido, com folga. Curto demais gera `Prazo de conexão esgotado` e posse liberada sem necessidade. O RNF04 pede `CONNECT_TIMEOUT` ≤ `PRAZO_CONEXAO` |
| `LIMIAR_CONEXAO_FRACA` (-78 dBm) | central | Ande com a pulseira se afastando de um coletor e anote o RSSI (painel, valor bruto) no ponto em que a FC começa a falhar e no ponto em que a conexão cai | Se a conexão **cai antes** do `conexao-fraca`, suba o limiar (ex.: -75). Se o handoff sai onde a FC ainda chega bem, desça |
| `MARGEM_HISTERESE` (5 dB) | central | Fique 2 min parado na divisa entre dois coletores | Se a posse alterna entre eles, aumente |
| `CARENCIA_HANDOFF` (30 s) | central | No mesmo teste da divisa: `Handoff adiado` e handoffs seguidos com a origem vencendo | Aumente se a origem vence e logo perde de novo |

Para o `.env` novo valer, rode `docker compose up -d` na máquina. Anote os valores usados na ficha da seção 8.6.

### 8.3 Gravação

Na central, antes de começar o percurso:

```bash
mkdir -p logs
INICIO=$(date +%FT%T)

# frequência cardíaca, uma mensagem JSON por linha (para a cobertura, seção 8.5)
docker run --rm --network host eclipse-mosquitto:2 mosquitto_sub -h localhost \
  -t 'pulseiras/+/frequencia-cardiaca' -F '%p' > "logs/fc-$(date +%F_%H%M).jsonl" &

# tudo o que passa no broker, para investigar depois
docker run --rm --network host eclipse-mosquitto:2 mosquitto_sub -h localhost \
  -t '#' -F '%I|%t|%p' > "logs/broker-$(date +%F_%H%M).txt" &
```

Durante o percurso, anote o horário de cada evento ("14:02 entrei na sala 2", "14:10 tirei o coletor-sala-3 da tomada").

Ao terminar:

```bash
kill %1 %2
docker compose logs --timestamps --since "$INICIO" coordenador > "logs/coordenador-$(date +%F_%H%M).log"
# em cada coletor, e depois copie para a central com scp:
docker compose logs --timestamps --since "<início>" coletor > "logs/$(uname -n)-$(date +%F_%H%M).log"
```

### 8.4 Roteiro

| # | Cenário | O que provoca | Critério |
|---|---|---|---|
| 1 | Percurso normal, passando pela área de todos os coletores, repetido até **30 handoffs** | Handoff por movimento | Lacuna p50 ≤ 10 s e p95 ≤ 20 s (RNF04) |
| 2 | 2 min parado na divisa entre dois coletores | Disputa com favorito, carência | Sem troca de posse em laço |
| 3 | Sair da área de todos os coletores por 3 min e voltar | Reconexão esgotada, `nao-encontrado`, nova disputa | Volta a ser atendida sozinha |
| 4 | Tirar da tomada o coletor ativo no meio do percurso | Pulseira órfã | Outro coletor assume |
| 5 | Reiniciar o coordenador no meio do percurso (`docker compose restart coordenador`) | Reinício (RF19) | A conexão não cai; `Posse republicada` |
| 6 | Desligar o broker por 10 s e por 30 s | Prazo sem broker (RF11) | Comportamento do F6 |
| 7 | Tirar a pulseira do braço por 30 s | Sem leitura de FC (RF16) | Aviso no log e "sem leitura" no painel |

A meta do RNF04 com 4 handoffs simultâneos fica para quando o coletor atender várias pulseiras.

### 8.5 Análise

**Lacuna dos handoffs (RNF04):**

```bash
grep 'Handoff concluído' logs/coordenador-*.log | grep -v 'até agora' \
  | grep -o 'Lacuna: [0-9.]*' | awk '{print $2}' > logs/lacunas.txt

python3 - <<'EOF'
import statistics
lacunas = sorted(float(linha) for linha in open("logs/lacunas.txt"))
p95 = statistics.quantiles(lacunas, n=20, method="inclusive")[-1] if len(lacunas) > 1 else lacunas[-1]
print(f"handoffs: {len(lacunas)} | p50: {statistics.median(lacunas):.1f} s | p95: {p95:.1f} s | máx: {lacunas[-1]:.1f} s")
EOF
```

**Cobertura da frequência cardíaca e trocas de coletor:**

```bash
python3 - logs/fc-*.jsonl <<'EOF'
import json, sys
from collections import defaultdict
from datetime import datetime

segundos = defaultdict(set)
trocas = []
anterior = {}
for caminho in sys.argv[1:]:
    for linha in open(caminho):
        try:
            mensagem = json.loads(linha)
            pulseira, coletor = mensagem["conteudo"]["pulseira"], mensagem["conteudo"]["coletor"]
            momento = datetime.fromisoformat(mensagem["timestamp"])
        except (ValueError, KeyError):
            continue
        segundos[pulseira].add(int(momento.timestamp()))
        if anterior.get(pulseira) not in (None, coletor):
            trocas.append(f"{momento:%H:%M:%S} {pulseira}: {anterior[pulseira]} -> {coletor}")
        anterior[pulseira] = coletor

for pulseira, s in segundos.items():
    total = max(s) - min(s) + 1
    print(f"{pulseira} | cobertura: {len(s) / total:.1%} ({len(s)} de {total} s) | meta: 95%")
print(f"trocas de coletor na FC: {len(trocas)}")
print("\n".join(trocas))
EOF
```

Cada troca de coletor na FC deve corresponder a um `Handoff concluído` ou a uma órfã no log do coordenador. Uma troca que volta ao coletor anterior em poucos segundos indica dois coletores publicando: investigue no `logs/broker-*.txt`.

**Contagem de eventos:**

```bash
for evento in 'Handoff concluído' 'Acima da meta' 'Handoff cancelado' 'Handoff adiado' \
              'Disputa sem vencedor' 'Prazo de conexão esgotado' 'Posse devolvida à origem' \
              'Posse perdida sem handoff' 'Mensagem antiga ignorada' 'Coletor indisponível'; do
  printf '%-28s %s\n' "$evento" "$(cat logs/*.log | grep -c "$evento")"
done
```

`Posse perdida sem handoff` e `Mensagem antiga ignorada` deveriam ficar em zero num percurso sem quedas provocadas. A segunda quase sempre é relógio fora de sincronia.

### 8.6 Ficha do teste

| Campo | Valor |
|---|---|
| Data e local | |
| Versão (`git rev-parse --short HEAD`) | |
| Coletores (hostname → posição) | |
| `LIMIAR_CONEXAO_FRACA` / `MINIMO_AMOSTRAS` / `MARGEM_HISTERESE` / `PRAZO_CONEXAO` / `CARENCIA_HANDOFF` | |
| Handoffs / p50 / p95 / máximo da lacuna | |
| Cobertura de FC | |
| Cancelados / sem vencedor / prazos esgotados | |
| Anomalias (com horário) | |

---

## 9. Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `bluetooth-indisponivel` / `Falha ao iniciar varredura` | Bluetooth desligado ou bloqueado; no Docker, `bluetoothd` parado no host | `rfkill unblock bluetooth`, `bluetoothctl power on`, `systemctl status bluetooth` |
| `rssi-indisponivel` com "sem permissão" | `btmgmt` sem `CAP_NET_ADMIN` | Seção 2.2 |
| `rssi-indisponivel` com "Not Connected" estando conectado | O código consulta o `btmgmt` como endereço LE público (`-t 1`) | `bluetoothctl info <MAC>` e confira o `AddressType`; se for `random`, registre |
| A pulseira não aparece em nenhum coletor | Conectada ao celular ou presa a um processo que morreu; desligada; bateria | `bluetoothctl info <MAC>`, `bluetoothctl disconnect <MAC>`, desligar o Bluetooth do celular |
| O coletor conecta e desconecta do broker em laço | Dois clientes com o mesmo id (hostname repetido, `COLETOR_ID` igual ou `main.py` rodando junto) | Seção 2.4 |
| `Mensagem ignorada \| ... não é o coordenador` | `COORDENADOR_ID` diferente entre as máquinas | Igualar no `.env` |
| `Mensagem antiga ignorada` em WARNING | O relógio de um coletor voltou no tempo | Seção 2.5 |
| `Disputa sem vencedor` | Amostras abaixo de `MINIMO_AMOSTRAS`, coletores offline ou pulseira presa a uma conexão | Seção 8.2 e F3 |
| `Prazo de conexão esgotado` frequente | `PRAZO_CONEXAO` menor que o tempo real de conexão | Seção 8.2 |
| Coletor fantasma no coordenador ou no painel | Presença retida de um id antigo | Seção 5.4 |
| `Pulseira ... não configurada (PULSEIRAS)` | MAC fora de `PULSEIRAS` na central | Corrigir o `.env` da central |
| O coordenador não sobe: `precisa conhecer as pulseiras` | `PULSEIRAS` e `TARGET_ADDRESS` vazios na central | Corrigir o `.env` da central |
| Handoff nunca acontece | Sem RSSI da conexão (`btmgmt`) ou limiar baixo demais | Seção 2.2; F12 para provar que o caminho funciona |

---

## 10. Encerrar

```bash
docker compose stop        # para tudo e mantém as retidas no volume
docker compose down        # remove os containers; os volumes ficam
docker compose down -v     # remove também os volumes: posse, presença e sessão do coordenador
```

No coletor nativo, use `Ctrl+C`. O `app.py` trata `SIGINT` e `SIGTERM`, então tanto o `Ctrl+C` quanto o `docker stop` liberam a pulseira e publicam `offline` antes de sair.
