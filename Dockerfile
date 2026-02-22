FROM apache/airflow:3.0.2

# Install system dependencies for Playwright (Chromium) and DVC (git) as root
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libdbus-1-3 libxkbcommon0 \
        libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 \
        libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
        libx11-xcb1 && \
    rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python dependencies (includes dvc)
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright's Chromium browser
RUN playwright install chromium