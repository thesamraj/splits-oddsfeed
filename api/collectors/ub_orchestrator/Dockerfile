FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf-2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for both collectors
RUN pip install --no-cache-dir \
    requests==2.31.0 \
    redis==5.0.1 \
    playwright==1.41.0 \
    aiohttp==3.9.1

# Install Playwright browsers
RUN playwright install chromium

# Copy all collector scripts
COPY ub_http/ub_http_collector.py /app/collectors/ub_http/
COPY ub_cdp/ub_cdp_collector.py /app/collectors/ub_cdp/
COPY ub_orchestrator/ub_orchestrator.py /app/

CMD ["python", "-u", "ub_orchestrator.py"]
