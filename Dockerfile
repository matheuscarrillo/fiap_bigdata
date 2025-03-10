FROM python:3.10-slim

# Instalar dependências do sistema, incluindo aquelas necessárias para o Chrome
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    ca-certificates \
    libgconf-2-4 \
    libasound2 \
    libcurl3-gnutls \
    libcurl3-nss \
    libcurl4 \
    libnspr4 \
    libnss3 \
    libxss1 \
    libgbm1 \
    xdg-utils \
    fonts-liberation \
    libappindicator3-1 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    libgdk-pixbuf2.0-0 \
    libvulkan1 \
    --no-install-recommends

# Baixar e instalar o Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && dpkg -i google-chrome-stable_current_amd64.deb \
    && apt-get install -f -y \
    && rm google-chrome-stable_current_amd64.deb

# Instalar dependências do Python
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar o código da aplicação
COPY . /app/
WORKDIR /app

# Definir variáveis de ambiente para o Chrome
ENV CHROME_BIN=/usr/bin/google-chrome-stable
ENV CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Comando para rodar a aplicação
CMD ["python", "scrap_b3.py"]


