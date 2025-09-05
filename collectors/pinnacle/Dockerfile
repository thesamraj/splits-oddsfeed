FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir redis requests

COPY pinnacle_collector.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_URL=redis://broker:6379/0
ENV CHANNEL=odds.raw.pinnacle
ENV INTERVAL=30

CMD ["python", "pinnacle_collector.py"]
