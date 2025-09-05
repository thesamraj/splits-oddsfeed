FROM python:3.11-slim

WORKDIR /app

RUN pip install redis psycopg2-binary

COPY kambi_processor.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds
ENV DB_PASS=oddspass

CMD ["python", "kambi_processor.py"]
