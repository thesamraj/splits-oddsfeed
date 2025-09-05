FROM mcr.microsoft.com/playwright/python:v1.48.0-focal

WORKDIR /app

RUN pip install playwright redis

COPY pinnacle_browser.py .

ENV PYTHONUNBUFFERED=1
ENV REDIS_URL=redis://broker:6379/0
ENV CHANNEL=odds.raw.pinnacle
ENV HEADLESS=true

CMD ["python", "pinnacle_browser.py"]
