FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir redis
COPY shim.py /app/shim.py
ENV PYTHONUNBUFFERED=1
CMD ["python","/app/shim.py"]
