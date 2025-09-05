FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install redis requests prometheus-client psycopg2-binary flask pyyaml

# Copy base modules first
COPY collectors/base/ /app/collectors/base/
COPY infra/ /app/infra/
COPY realness/ /app/realness/

# Copy collector code
COPY collectors/bovada_real/bovada_real_enhanced.py /app/

ENV PYTHONUNBUFFERED=1
ENV REDIS_HOST=broker
ENV COLLECTION_INTERVAL=60

CMD ["python", "bovada_real_enhanced.py"]
