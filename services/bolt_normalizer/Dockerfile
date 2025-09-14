FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    flask==3.0.0 \
    prometheus_client==0.19.0 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9

# Copy service code
COPY bolt_normalizer.py /app/

# Expose port
EXPOSE 8000

# Run service
CMD ["python", "/app/bolt_normalizer.py"]