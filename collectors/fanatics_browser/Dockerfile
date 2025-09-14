FROM mcr.microsoft.com/playwright/python:v1.40.0-focal

WORKDIR /app

# Install Python dependencies
RUN pip install flask prometheus_client redis requests playwright

# Install Playwright browsers
RUN python -m playwright install chromium

# Copy collector
COPY collectors/fanatics_browser/collector_dom.py /app/collector.py

# Environment
ENV FANATICS_USE_DOM=true

EXPOSE 8000
CMD ["python", "/app/collector.py"]