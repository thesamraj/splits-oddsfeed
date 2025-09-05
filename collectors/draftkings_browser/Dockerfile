FROM mcr.microsoft.com/playwright/python:v1.40.0-focal

WORKDIR /app

RUN pip install redis playwright

COPY dk_browser.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV REDIS_PORT=6379
ENV INTERVAL=60

CMD ["python", "dk_browser.py"]
