FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    requests \
    redis \
    flask \
    playwright \
    beautifulsoup4 \
    lxml

# Install Playwright browser
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code
COPY sh_scraper.py /app/sh_scraper.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/sh_scraper.py"]
