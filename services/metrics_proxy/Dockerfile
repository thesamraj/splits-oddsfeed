FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir flask requests psycopg2-binary

# Copy the proxy script
COPY metrics_proxy/metrics_proxy.py .

# Expose port
EXPOSE 8000

# Run the proxy
CMD ["python", "metrics_proxy.py"]
