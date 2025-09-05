FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy
WORKDIR /app
COPY brand_cookie_bootstrap.py /app/
RUN pip install --no-cache-dir --upgrade pip playwright
CMD ["python","/app/brand_cookie_bootstrap.py"]
