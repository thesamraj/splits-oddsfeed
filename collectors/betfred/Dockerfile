FROM mcr.microsoft.com/playwright/python:v1.40.0-focal
WORKDIR /app
RUN pip install flask prometheus_client redis playwright httpx
RUN python -m playwright install chromium
COPY collector_playwright.py /app/collector.py
EXPOSE 8000
CMD ["python", "/app/collector.py"]
