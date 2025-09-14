FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (already in base image but ensure it's there)
RUN playwright install chromium

# Copy application
COPY main.py .

# Environment
ENV PYTHONUNBUFFERED=1

# Run
CMD ["python", "-u", "main.py"]