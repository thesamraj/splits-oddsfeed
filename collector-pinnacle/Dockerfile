FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy dependency files and README
COPY pyproject.toml poetry.lock* README.md ./

# Configure poetry and install dependencies (without installing the package itself)
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-root

# Copy source code
COPY src/ ./src/

# Install the package itself
RUN poetry install --only-root

# Expose metrics port
EXPOSE 9110

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9110/metrics || exit 1

# Run the collector
CMD ["python", "-m", "collector_pinnacle.main"]
