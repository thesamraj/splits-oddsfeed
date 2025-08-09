from prometheus_client import Counter, Histogram, start_http_server
from fastapi import FastAPI, Request
import time


REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["endpoint", "method", "status_code"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["endpoint", "method"]
)


def setup_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        start_time = time.time()
        
        response = await call_next(request)
        
        duration = time.time() - start_time
        
        REQUEST_COUNT.labels(
            endpoint=request.url.path,
            method=request.method,
            status_code=response.status_code
        ).inc()
        
        REQUEST_LATENCY.labels(
            endpoint=request.url.path,
            method=request.method
        ).observe(duration)
        
        return response