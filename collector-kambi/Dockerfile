FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry

COPY pyproject.toml .
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

COPY src/ ./

EXPOSE 9107

CMD ["python", "/app/collector_kambi/main.py"]
