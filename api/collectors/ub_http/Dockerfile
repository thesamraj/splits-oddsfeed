FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests==2.31.0 \
    redis==5.0.1

COPY ub_http_collector.py /app/

CMD ["python", "-u", "ub_http_collector.py"]
