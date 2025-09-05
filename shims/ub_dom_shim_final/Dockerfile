FROM python:3.11-slim
RUN pip install --no-cache-dir redis
WORKDIR /app
COPY shim.py /app/shim.py
CMD ["python","-u","/app/shim.py"]
