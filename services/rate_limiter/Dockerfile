FROM python:3.11-slim

WORKDIR /app

RUN pip install redis

COPY rate_limiter.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379

CMD ["python", "rate_limiter.py"]
