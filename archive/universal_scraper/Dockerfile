FROM python:3.11-slim

WORKDIR /app

RUN pip install redis requests beautifulsoup4

COPY universal_scraper.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV INTERVAL=30
ENV BOOK=unknown

CMD ["python", "universal_scraper.py"]
