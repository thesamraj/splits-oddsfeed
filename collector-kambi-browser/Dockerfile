# Official image ships Playwright + browsers preinstalled (Chromium/WebKit/Firefox)
# Pick a modern tag; update if needed later.
FROM mcr.microsoft.com/playwright/python:v1.54.0

WORKDIR /app

# System env for cleaner logs
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Bring in Python project metadata first for better layer cache
COPY pyproject.toml ./

# Use pip (simple) instead of poetry here to keep image small and fast
# If your pyproject is PEP621 (poetry), we can still use pip via PEP 517/518
RUN python -m pip install --upgrade pip wheel setuptools \
 && python -m pip install "build>=1.2.1" \
 && python -m pip wheel --no-deps .

# Install runtime deps directly (faster than building wheel of the whole app)
# If build above created a wheel, this is harmless; we'll install src next.
RUN python -m pip install --no-cache-dir -U \
      prometheus-client \
      redis \
      playwright \
      httpx \
      uvloop \
      orjson

# App source
COPY src/ ./src/

# Default command runs the relay
WORKDIR /app/src
CMD ["python","-u","-m","collector_kambi_browser.main"]
