FROM mcr.microsoft.com/playwright/python:v1.48.0-focal

WORKDIR /app

RUN pip install playwright redis

COPY betmgm_browser.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_URL=redis://broker:6379/0
ENV CHANNEL=odds.raw.betmgm
ENV HEADLESS=true

CMD ["python", "betmgm_browser.py"]
