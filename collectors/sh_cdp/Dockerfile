FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /app

# Install Python packages including playwright
RUN pip install --no-cache-dir playwright redis flask

COPY sh_cdp_scraper.py /app/

ENV PYTHONUNBUFFERED=1
CMD ["python3", "sh_cdp_scraper.py"]
