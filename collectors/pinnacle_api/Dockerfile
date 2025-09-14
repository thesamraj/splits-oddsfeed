FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir flask requests redis prometheus-client
COPY collector.py /app/collector.py
CMD ["python", "/app/collector.py"]