FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry

COPY pyproject.toml .
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

COPY src/ ./

EXPOSE 8099

CMD ["uvicorn", "proxy_manager.main:app", "--host", "0.0.0.0", "--port", "8099"]
