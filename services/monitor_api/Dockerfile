FROM python:3.11-slim

WORKDIR /app

RUN pip install flask flask-cors redis psycopg2-binary

COPY monitor_api.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds
ENV DB_PASS=odds

CMD ["python", "monitor_api.py"]
