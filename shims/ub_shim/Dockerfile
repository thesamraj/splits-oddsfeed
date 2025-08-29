FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir redis flask
COPY ub_shim.py /app/ub_shim.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/ub_shim.py"]
