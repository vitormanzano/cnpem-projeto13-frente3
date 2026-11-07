FROM python:3.11-slim

# Evita geração de bytecode .pyc e garante logs em tempo real sem buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src/gateway

# Pacotes necessários para comunicação Bluetooth (BlueZ e D-Bus) no Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    bluez \
    dbus \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalação das dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cópia do código-fonte do projeto
COPY src/ ./src/

# Comando padrão para inicializar o gateway BLE
CMD ["python", "src/gateway/main.py"]
