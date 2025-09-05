FROM mcr.microsoft.com/playwright/python:v1.48.0-focal

WORKDIR /app

RUN pip install playwright redis

COPY pointsbet_browser.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_URL=redis://broker:6379/0
ENV CHANNEL=odds.raw.pointsbet
ENV HEADLESS=true

CMD ["python", "pointsbet_browser.py"]
