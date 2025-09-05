FROM python:3.11-slim

WORKDIR /app

RUN pip install redis

COPY deduplicator.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV CACHE_TTL=300

CMD ["python", "deduplicator.py"]
