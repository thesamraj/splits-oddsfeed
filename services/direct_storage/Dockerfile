FROM python:3.11-slim

WORKDIR /app

RUN pip install redis psycopg2-binary

COPY direct_storage.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds

CMD ["python", "direct_storage.py"]
