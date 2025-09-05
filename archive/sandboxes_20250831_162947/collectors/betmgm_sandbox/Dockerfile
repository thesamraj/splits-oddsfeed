FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for curl-cffi and playwright
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libssl-dev \
    libcurl4-openssl-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages including curl-cffi
RUN pip install --no-cache-dir \
    requests \
    redis \
    psycopg2-binary \
    beautifulsoup4 \
    curl-cffi \
    playwright \
    cloudscraper

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps

COPY . .

CMD ["python", "mgm_cffi_collector.py"]
