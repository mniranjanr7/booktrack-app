import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from psycopg_pool import AsyncConnectionPool


# 1. Setup Database Connection String
DB_CONN_STR = (
    f"host={os.getenv('DB_HOST')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')} "
    f"connect_timeout=2"
)

# 2. Define Lifespan for Startup and Shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize the pool
    app.state.is_loaded = False
    app.state.pool = AsyncConnectionPool(conninfo=DB_CONN_STR, min_size=1, max_size=5, open=True)
    # await app.async_pool.open()
    app.state.is_loaded = True
    yield
    # Shutdown: Gracefully close the pool
    await app.state.pool.close()

# 3. Initialize FastAPI with Lifespan
app = FastAPI(title="BookTrack API", lifespan=lifespan)

# Liveness Probe
@app.get("/healthz")
def healthz():
    """
    Liveness probe.
    Only checks that the app process is running.
    """
    return {"status": "ok"}

# Readiness Probe

@app.get("/readyz")
async def readyz():
    if not hasattr(app.state, "pool"):
        raise HTTPException(status_code=503, detail="Pool not initialized")
    
    try:
        async with app.state.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1;")
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Database not ready")

@app.get("/books")
async def list_books():
    try:
        async with app.state.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, title FROM books;")
                rows = await cur.fetchall()
                return [{"id": r[0], "title": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch books")
    
# Startup Probe
@app.get("/startup")
async def startup():
    """
    Checks if the application initialization is complete.
    """
    if not getattr(app.state, "is_loaded", False):
        raise HTTPException(status_code=503, detail="Initializing")
    return {"status": "started"}
