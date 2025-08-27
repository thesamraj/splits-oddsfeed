FROM python:3.11-slim
RUN pip install --no-cache-dir requests redis flask
WORKDIR /app
COPY br_prematch.py /app/br_prematch.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "br_prematch.py"]
