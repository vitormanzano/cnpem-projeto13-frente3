# CNPEM - Projeto 13 Frente 3
**Sistema de Monitoramento Cardíaco e Detecção de Estresse para o NB4**

Módulo gateway de captura e comunicação via Bluetooth Low Energy (BLE) com o sensor Polar Verity Sense, desenvolvido para integração e transmissão de dados fisiológicos no ambiente do laboratório de biossegurança máxima NB4 (CNPEM).

---
## 📋 Sumário
- [Arquitetura e Tecnologias](#-arquitetura-e-tecnologias)
- [Estrutura de Pastas](#-estrutura-de-pastas)
- [Diferença Fundamental: macOS vs. Linux](#-diferença-fundamental-macos-vs-linux)
- [Como Rodar no macOS (Desenvolvimento Local)](#-como-rodar-no-macos-desenvolvimento-local)
- [Como Rodar no Linux (Docker & Docker Compose)](#-como-rodar-no-linux-docker--docker-compose)
- [Como Rodar no Linux Nativo (Sem Docker)](#-como-rodar-no-linux-nativo-sem-docker)
- [Suíte de Testes BLE](#-suíte-de-testes-ble)
- [Variáveis de Ambiente](#-variáveis-de-ambiente)

---

## 🛠 Arquitetura e Tecnologias

- **Linguagem:** Python 3.11+
- **Comunicação BLE:** [Bleak](https://bleak.readthedocs.io/) (utiliza CoreBluetooth no macOS e BlueZ/D-Bus no Linux)
- **Containerização:** Docker e Docker Compose
- **Dispositivo Alvo:** Polar Verity Sense (Sensor óptico de frequência cardíaca e PPG)

---

## 📂 Estrutura de Pastas

```text
cnpem-projeto13-frente3/
├── .dockerignore
├── Dockerfile                  # Imagem Python com dependências do BlueZ/D-Bus
├── docker-compose.yml          # Configuração de privilégios de hardware e volumes
├── requirements.txt            # Dependências Python (bleak)
├── README.md
├── docs/                       # Documentação técnica e especificações
└── src/
    └── gateway/
        ├── __init__.py
        ├── main.py             # Ponto de entrada e menu interativo de testes
        └── bluetooth/
            ├── __init__.py
            └── scanner.py      # Rotinas de varredura, filtros e conexão GATT
```

---

## ⚠️ Diferença Fundamental: macOS vs. Linux

A biblioteca `bleak` interage com o hardware Bluetooth de maneiras distintas dependendo do sistema operacional:

| Característica | macOS | Linux (Container ou Host) |
| :--- | :--- | :--- |
| **Pilha de Bluetooth** | Apple CoreBluetooth nativo | BlueZ via D-Bus (`/run/dbus/system_bus_socket`) |
| **Identificação do Sensor** | UUID (ex: `F1142908-CB92-...`) | Endereço MAC (ex: `C0:4E:01:XX:XX:XX`) |
| **Suporte em Docker** | ❌ Não suportado pelo Docker Desktop (VM sem passthrough de BLE) |  Suportado com `network_mode: host` e `privileged: true` |
| **Modo de Uso Recomendado** | **Ambiente virtual local (`.venv`)** | **Docker Compose ou Nativo** |

---

## 🍏 Como Rodar no macOS (Desenvolvimento Local)

No macOS, **não use o Docker** para testes BLE, pois o Docker Desktop roda dentro de uma máquina virtual que não possui acesso ao chip Bluetooth do Mac. Execute o projeto diretamente no terminal:

### 1. Criar e Ativar o Ambiente Virtual

```bash
# Na raiz do projeto:
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Instalar as Dependências

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Executar o Gateway

```bash
python src/gateway/main.py
```

> **Atenção:** Na primeira execução, o macOS exibirá uma caixa de diálogo solicitando permissão para que o seu Terminal ou IDE acesse o **Bluetooth**. Clique em **"Permitir"** (ou configure em *Ajustes do Sistema > Privacidade e Segurança > Bluetooth*).

---

## 🐧 Como Rodar no Linux (Docker & Docker Compose)

Este é o modo projetado para os computadores e gateways do laboratório (Ubuntu, Debian ou Raspberry Pi OS).

### 1. Pré-requisitos no Sistema Host

Certifique-se de que os daemons de D-Bus e BlueZ estão ativos no host:

```bash
sudo systemctl enable --now dbus
sudo systemctl enable --now bluetooth

# Verifique se o adaptador Bluetooth está ativo (UP)
hciconfig hci0
# Caso esteja DOWN:
# sudo hciconfig hci0 up

# Verificar se o usuário está no grupo bluetooth
# groups
# Caso não esteja: 
# sudo usermod -aG bluetooth $USER
# Verificar permissão está correta. Deve estar com leitura e escrita para todos os usuários, ou para o grupo associado
# ls -l /var/run/dbus/system_bus_socket
```

### 2. Configurar o Endereço do Dispositivo

No Linux, o Bluetooth utiliza o endereço MAC físico. Você pode passar o MAC via variável de ambiente ou descomentar no `docker-compose.yml`:

```bash
export TARGET_ADDRESS="C0:4E:01:XX:XX:XX"
```

### 3. Executar com Docker Compose

Como o [`main.py`](file:///Users/vimanzanovd/dev/cnpem-projeto13-frente3/src/gateway/main.py) possui um menu interativo (`input`), execute com o terminal acoplado:

```bash
# Execução interativa com terminal anexado:
docker compose run --rm gateway

# Ou para construir e subir a imagem em background:
docker compose up -d --build
```

---

## 🐧 Como Rodar no Linux Nativo (Sem Docker)

Se preferir rodar sem containers no Linux:

```bash
# 1. Instalar bibliotecas de sistema do BlueZ
sudo apt-get update && sudo apt-get install -y bluez libbluetooth-dev dbus

# 2. Criar ambiente virtual e instalar pacotes
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Executar
python src/gateway/main.py
```

---

## 🧪 Suíte de Testes BLE

Ao iniciar o [`main.py`](file:///Users/vimanzanovd/dev/cnpem-projeto13-frente3/src/gateway/main.py), o seguinte menu interativo será exibido:

```text
##################################################
      SUÍTE DE TESTES BLE - DIAGNÓSTICO FRENTE 3
##################################################
1. Executar Teste 1: Scan Simples (Verificar MAC/UUIDs)
2. Executar Teste 2: Filtrar por Nome/Serviço (Critérios Alternativos)
3. Executar Teste 3: Testar Conexão GATT e Visibilidade no celular
4. Executar TODOS os testes em sequência
0. Sair
```

### Descrição dos Testes:
* **Teste 1 (Scan Simples):** Realiza uma varredura geral de 5 segundos listando todos os dispositivos BLE ao alcance, seu endereço (MAC/UUID), nome anunciado e intensidade do sinal (RSSI em dBm).
* **Teste 2 (Critérios Alternativos):** Localiza o dispositivo filtrando pelo nome do pacote de anúncio (`TARGET_NAME`, ex: `"Polar Verity"`) ou UUID de serviço (`TARGET_SERVICE_UUID`). Essencial quando o sensor utiliza endereço MAC aleatório/rotativo (RPA).
* **Teste 3 (Conexão e Visibilidade Concorrente):** Estabelece uma conexão GATT de 20 segundos com o sensor. Permite validar no aplicativo *nRF Connect* do smartphone se o sensor fica invisível para novas varreduras enquanto conectado (relação 1:1 e suporte a handoff).
* **Teste 4 (Todos):** Executa os três testes de diagnóstico em sequência.

---

## ⚙️ Variáveis de Ambiente

O aplicativo aceita customizações via variáveis de ambiente, facilitando a troca de parâmetros sem alterar o código:

| Variável | Padrão | Descrição |
| :--- | :--- | :--- |
| `TARGET_ADDRESS` | `F1142908-CB92-...` | Endereço MAC (Linux) ou UUID (macOS) do sensor alvo |
| `TARGET_NAME` | `Polar Verity` | Nome local anunciado pelo dispositivo no advertising BLE |
| `TARGET_SERVICE_UUID`| `F1142908-CB92-...` | UUID do serviço GATT anunciado para validação de filtro |
| `RECONEXAO_MAX_TENTATIVAS` | `5` | Máximo de tentativas de reconexão após uma queda inesperada da pulseira (RF05) |
| `RECONEXAO_INTERVALO` | `3` | Segundos de espera entre as tentativas de reconexão (RF05) |
| `PYTHONUNBUFFERED` | `1` | Garante envio imediato de logs para o terminal/container |

Exemplo de uso via linha de comando:
```bash
TARGET_ADDRESS="C0:4E:01:23:45:67" TARGET_NAME="Polar Verity Sense" python src/gateway/main.py
```
