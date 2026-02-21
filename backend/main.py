import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from psycopg_pool import AsyncConnectionPool
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ---------------------------
# Database Connection String
# ---------------------------

DB_CONN_STR = (
    f"host={os.getenv('DB_HOST')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')} "
    f"connect_timeout=2"
)

# ---------------------------
# Prometheus Metrics
# ---------------------------

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"]
)

DB_FAILURES = Counter(
    "db_connection_failures_total",
    "Total database connection failures"
)

# ---------------------------
# Lifespan
# ---------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.is_loaded = False
    app.state.pool = AsyncConnectionPool(
        conninfo=DB_CONN_STR,
        min_size=1,
        max_size=5,
        open=True
    )
    app.state.is_loaded = True
    yield
    await app.state.pool.close()

app = FastAPI(title="BookTrack API", lifespan=lifespan)

# ---------------------------
# Middleware for Metrics
# ---------------------------

@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    duration = time.time() - start_time
    endpoint = request.url.path

    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status=response.status_code
    ).inc()

    REQUEST_LATENCY.labels(
        method=request.method,
        endpoint=endpoint
    ).observe(duration)

    return response

# ---------------------------
# Probes
# ---------------------------

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/readyz")
async def readyz():
    if not hasattr(app.state, "pool"):
        raise HTTPException(status_code=503, detail="Pool not initialized")

    try:
        async with app.state.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1;")
        return {"status": "ready"}
    except Exception:
        DB_FAILURES.inc()
        raise HTTPException(status_code=503, detail="Database not ready")

@app.get("/startup")
async def startup():
    if not getattr(app.state, "is_loaded", False):
        raise HTTPException(status_code=503, detail="Initializing")
    return {"status": "started"}

# ---------------------------
# Business Endpoint
# ---------------------------

@app.get("/books")
async def list_books():
    try:
        async with app.state.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT title, id FROM books;")
                rows = await cur.fetchall()
                return [{"title": r[0], "id": r[1]} for r in rows]
    except Exception:
        DB_FAILURES.inc()
        raise HTTPException(status_code=500, detail="Failed to fetch books")

# ---------------------------
# Metrics Endpoint
# ---------------------------

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)