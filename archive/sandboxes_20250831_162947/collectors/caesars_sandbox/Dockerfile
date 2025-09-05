FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    redis \
    psycopg2-binary \
    beautifulsoup4

COPY . .

CMD ["python", "caesars_collector.py"]
