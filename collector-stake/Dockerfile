FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry

COPY pyproject.toml .
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

RUN playwright install chromium
RUN playwright install-deps

COPY src/ ./

EXPOSE 9105

CMD ["python", "collector_stake/main.py"]
