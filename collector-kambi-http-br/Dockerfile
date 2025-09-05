FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir requests redis flask

# Copy poller script
COPY poller.py /app/

# Expose healthz port
EXPOSE 9126

CMD ["python", "poller.py"]
