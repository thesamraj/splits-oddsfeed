FROM python:3.11-slim

WORKDIR /app

RUN pip install redis psycopg2-binary requests

COPY alert_service.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds
ENV DB_PASS=oddspass
ENV CHECK_INTERVAL=60

CMD ["python", "alert_service.py"]
