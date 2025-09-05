FROM python:3.11-slim

WORKDIR /app

RUN pip install redis

COPY dkfd_real.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV INTERVAL=30

CMD ["python", "dkfd_real.py"]
