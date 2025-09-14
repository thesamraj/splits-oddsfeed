FROM python:3.11-slim

WORKDIR /app

# Install CA certificates for TLS trust
RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector code
COPY kambi_collector.py .
COPY kambi_api.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "kambi_collector.py"]
