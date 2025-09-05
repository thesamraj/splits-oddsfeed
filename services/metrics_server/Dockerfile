FROM python:3.11-slim

WORKDIR /app

RUN pip install flask prometheus-client redis psycopg2-binary

COPY metrics_server.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds
ENV DB_PASS=odds
ENV METRICS_PORT=9090

EXPOSE 9090

CMD ["python", "metrics_server.py"]
