FROM python:3.11-slim

WORKDIR /app

RUN pip install redis requests

COPY fanduel_api.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV INTERVAL=30

CMD ["python", "fanduel_api.py"]
