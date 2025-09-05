FROM python:3.11-slim

WORKDIR /app

RUN pip install psycopg2-binary

COPY db_cleaner.py .

ENV PYTHONUNBUFFERED=1
ENV DB_HOST=store
ENV DB_NAME=oddsfeed
ENV DB_USER=odds
ENV DB_PASS=oddspass
ENV RETENTION_HOURS=2
ENV INTERVAL_SECONDS=1800

CMD ["python", "db_cleaner.py"]
