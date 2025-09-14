FROM python:3.11-slim
WORKDIR /app
RUN pip install flask prometheus_client redis requests
COPY collector.py /app/
EXPOSE 8000
CMD ["python", "/app/collector.py"]
