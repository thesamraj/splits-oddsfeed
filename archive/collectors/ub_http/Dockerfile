FROM python:3.11-slim
RUN pip install --no-cache-dir requests redis
WORKDIR /app
COPY ub_http.py /app/ub_http.py
CMD ["python","-u","/app/ub_http.py"]
