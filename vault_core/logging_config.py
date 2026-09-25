"""Structured JSON logging configuration and Request ID tracing."""

import json
import logging
import sys
import time
import uuid
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from vault_core.metrics import HTTP_REQUESTS_TOTAL, HTTP_REQUEST_DURATION_SECONDS


class JsonFormatter(logging.Formatter):
    """Formats log records into single-line JSON structures."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include custom attributes if present
        for key in ("request_id", "bucket", "key", "version_id", "status_code", "duration_ms", "client_ip"):
            if hasattr(record, key):
                log_obj[key] = getattr(record, key)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def setup_structured_logging(level: str = "INFO") -> logging.Logger:
    """Configure the root logger with JSON formatting."""
    logger = logging.getLogger("vault")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = setup_structured_logging()


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to inject X-Request-ID and emit structured JSON access logs."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = req_id

        start_time = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Request-ID"] = req_id
            return response
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            logger.error(
                f"Unhandled exception processing {request.method} {request.url.path}: {exc}",
                extra={
                    "request_id": req_id,
                    "status_code": 500,
                    "duration_ms": round(duration_ms, 2),
                },
                exc_info=True
            )
            raise exc
        finally:
            duration_sec = time.perf_counter() - start_time
            duration_ms = duration_sec * 1000.0
            endpoint = request.url.path

            # Update Prometheus
            HTTP_REQUESTS_TOTAL.labels(method=request.method, endpoint=endpoint, status=str(status_code)).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(method=request.method, endpoint=endpoint).observe(duration_sec)

            # Emit structured access log
            logger.info(
                f"{request.method} {request.url.path} -> {status_code} in {duration_ms:.2f}ms",
                extra={
                    "request_id": req_id,
                    "status_code": status_code,
                    "duration_ms": round(duration_ms, 2),
                    "client_ip": request.client.host if request.client else "unknown",
                }
            )
