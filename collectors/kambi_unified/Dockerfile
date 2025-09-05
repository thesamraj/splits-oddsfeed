FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy collector code
COPY kambi_collector.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "kambi_collector.py"]
