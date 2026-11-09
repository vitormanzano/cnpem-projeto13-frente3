# Como executar o coletor e o broker

Passo a passo para subir o broker MQTT (HiveMQ), rodar o `app.py` como **coletor** e conferir, com um espião, as mensagens que chegam ao broker.

Todos os comandos são executados na **raiz do projeto**. São usados três terminais:

| Terminal | Função |
|---|---|
| 1 | Broker (HiveMQ) |
| 2 | Espião: mostra tudo o que passa pelo broker |
| 3 | Coletor (`app.py` com `PAPEL=coletor`) |

---

## 0. Pré-requisitos

1. Docker e Docker Compose instalados (no Ubuntu, o `start.sh` instala).
2. Arquivo `.env` na raiz, criado a partir do exemplo:

   ```bash
   cp .env.example .env
   ```

3. Para testar tudo **numa máquina só**, o broker é local:

   ```bash
   # no .env
   BROKER_HOST=localhost
   ```

4. O **id do coletor é o hostname da máquina** (não há mais `COLETOR_ID` no `.env`). Para conferir qual será o id:

   ```bash
   uname -n
   ```

   Cada coletor precisa de um hostname diferente; numa Raspberry, troque com `sudo hostnamectl set-hostname coletor-sala-1`.

---

## 1. Terminal 1: subir o broker

```bash
docker compose up -d hivemq
docker logs -f cnpem_hivemq
```

- Nomear o serviço `hivemq` ativa sozinho o profile `central` só para ele. Usar `--profile central up` subiria também o coordenador, que ainda não tem `executar()`.
- O broker escuta na porta `1883` da máquina.
- Se o container já estiver de pé, o `up -d` não muda nada. Para conferir: `docker ps --filter name=cnpem_hivemq`.

---

## 2. Terminal 2: abrir o espião

```bash
docker run --rm -it --network host eclipse-mosquitto:2 \
  mosquitto_sub -h localhost -t '#' -F '%I | %t | retida=%r | %p'
```

| Parte | O que faz |
|---|---|
| `--rm -it` | apaga o container ao sair; terminal interativo (Ctrl+C para parar) |
| `--network host` | `localhost` passa a ser a máquina, onde o HiveMQ expõe a porta 1883 |
| `eclipse-mosquitto:2` | imagem que traz o cliente `mosquitto_sub` (só o cliente é usado, não o broker Mosquitto) |
| `-h localhost` | endereço do broker |
| `-t '#'` | filtro de tópico: `#` = **todos os tópicos** |
| `-F` | formato da linha: `%I` horário, `%t` tópico, `%r` retida (1/0), `%p` conteúdo |

### Curingas de tópico do MQTT

| Curinga | Significado | Regra |
|---|---|---|
| `+` | exatamente **um** nível, qualquer valor | pode aparecer em qualquer posição |
| `#` | **zero ou mais** níveis até o fim | só pode ser o último |

Exemplos de filtro para trocar no `-t`:

| Filtro | O que mostra |
|---|---|
| `'#'` | tudo (exceto tópicos internos do broker, que começam com `$`) |
| `'coletores/+/presenca'` | presença de todos os coletores |
| `'coletores/<id>/#'` | tudo de um coletor |
| `'coletores/+/pulseiras/+'` | estado/RSSI de todos os coletores para todas as pulseiras |
| `'pulseiras/+/posse'` | posse de todas as pulseiras |

As aspas são necessárias: em alguns shells (zsh, por exemplo) o `#` pode virar comentário e o resto da linha seria ignorado.

---

## 3. Terminal 3: rodar o coletor

Escolha uma das opções.

### Opção A: Python direto (mais rápida para testar)

```bash
source .venv/bin/activate          # na primeira vez: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
PAPEL=coletor python src/gateway/app.py
```

### Opção B: Docker (igual ao que roda na Raspberry)

```bash
docker compose build coletor                                 # só na primeira vez, ou quando mudar requirements.txt/Dockerfile
docker compose run --rm coletor python src/gateway/app.py
```

- O serviço `coletor` já define `PAPEL=coletor` no `docker-compose.yml`.
- O `run` troca o `CMD` da imagem (que ainda é o `main.py`) pelo `app.py`. Para o `docker compose --profile coletor up` rodar o `app.py`, falta adicionar `command: ["python", "src/gateway/app.py"]` no `x-gateway` do compose.
- `./src` é montado no container: mudanças no código não exigem novo `build`.

Nas duas opções, `BROKER_HOST=localhost` funciona: na A o Python roda no host, e na B o container usa `network_mode: host`.

Para parar o coletor: **Ctrl+C**. O `app.py` também trata o `SIGTERM` (`docker stop`), então nos dois casos ele publica `offline` antes de sair.

---

## 4. O que deve aparecer no espião

Uma linha por mensagem, por exemplo:

```
2026-11-05T14:40:01 | coletores/<hostname>/presenca | retida=1 | {"versao": ..., "tipo": "presenca", "origem": "<hostname>", ..., "conteudo": {"estado": "online", "pulseira": null}}
```

| Momento | Mensagem esperada em `coletores/<hostname>/presenca` |
|---|---|
| Coletor conecta | `"estado": "online"` |
| A cada `PRESENCA_INTERVALO_COLETOR` segundos (padrão 5 s) | `"estado": "online"` (sinal de vida) |
| Ctrl+C ou `docker stop` no coletor | `"estado": "offline"`, e o log do coletor mostra `Coletor encerrado.` |

Logo que o espião conecta, chegam também as **mensagens retidas** de execuções anteriores (`retida=1`). Por isso aparece o último estado de cada coletor, inclusive de ids que não existem mais.

---

## 5. Teste extra: última vontade (queda sem aviso)

Com o coletor rodando, mate o processo sem dar chance de ele publicar o `offline`:

```bash
pkill -9 -f src/gateway/app.py                         # se usou a opção A
docker kill $(docker ps -q --filter name=coletor)      # se usou a opção B
```

Em até ~15 s (1,5 × o keepalive de 10 s) o **próprio broker** publica `"estado": "offline"` no lugar do coletor. Essa é a última vontade (last will) registrada pela `PresencaColetor` ao conectar.

---

## 6. Limpar uma presença retida antiga

Quando um coletor muda de id (outro hostname), a presença retida do id antigo continua no broker e aparece como um coletor fantasma. Para apagá-la, publique uma mensagem retida vazia no tópico:

```bash
docker run --rm --network host eclipse-mosquitto:2 \
  mosquitto_pub -h localhost -t 'coletores/<id-antigo>/presenca' -r -n
```

- `-r`: retida
- `-n`: mensagem vazia (apaga a retida daquele tópico)

---

## 7. Encerrar tudo

```bash
# Terminal 2 e 3: Ctrl+C
docker compose stop hivemq      # para o broker e mantém as mensagens retidas no volume
docker compose down             # remove os containers (os volumes hivemq-data/hivemq-log continuam)
```

---

## 8. Em várias máquinas

O repositório e o `docker-compose.yml` são os mesmos em todas as máquinas. O que muda é o `.env` de cada uma, de onde o compose lê o `COMPOSE_PROFILES`:

| Máquina | `.env` | O que sobe |
|---|---|---|
| PC central | `COMPOSE_PROFILES=central` | HiveMQ + coordenador |
| Raspberry 1 | `COMPOSE_PROFILES=coletor`<br>`BROKER_HOST=<IP do PC central>`<br>`TARGET_ADDRESS=<MAC da pulseira>` | coletor (id = hostname) |
| Raspberry 2 | igual à Raspberry 1 | coletor (id = hostname, diferente da 1) |

Em cada máquina: `docker compose up -d --build`. Isso só vai rodar o `app.py` depois que o `command:` for adicionado ao `x-gateway`, e o coordenador só vai funcionar depois que ele tiver `executar()`.

Para testar os dois papéis num PC só: `COMPOSE_PROFILES=central,coletor` e `BROKER_HOST=localhost`.
