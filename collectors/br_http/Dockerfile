FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests redis flask
COPY br_prematch.py /app/br_prematch.py
ENV PYTHONUNBUFFERED=1
CMD ["python","/app/br_prematch.py"]
