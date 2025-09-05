FROM python:3.11-slim

WORKDIR /app

RUN pip install redis

COPY universal_collector.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV BOOK=unknown
ENV INTERVAL=30

CMD ["python", "universal_collector.py"]
