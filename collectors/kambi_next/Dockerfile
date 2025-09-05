FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir redis requests

COPY kambi_next.py .

CMD ["python", "-u", "kambi_next.py"]
