FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests redis flask
COPY ub_collector.py /app/ub_collector.py
ENV PYTHONUNBUFFERED=1
CMD ["python","/app/ub_collector.py"]
