FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (in case Playwright is needed)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    flask \
    redis \
    prometheus-client \
    requests

# Optional: Install Playwright (will be used if HTTP fails)
RUN pip install --no-cache-dir playwright && \
    playwright install chromium && \
    playwright install-deps chromium || true

# Copy collector
COPY collector.py .

# Run collector
CMD ["python", "collector.py"]