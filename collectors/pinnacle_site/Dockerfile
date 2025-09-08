FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    flask \
    redis \
    prometheus-client \
    requests \
    playwright \
    certifi

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy collector
COPY collector.py .

# Run collector
CMD ["python", "collector.py"]