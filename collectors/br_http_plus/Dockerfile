FROM python:3.11-slim
RUN pip install --no-cache-dir aiohttp redis
WORKDIR /app
COPY collector.py /app/collector.py
ENV PYTHONUNBUFFERED=1
CMD ["python","/app/collector.py"]
