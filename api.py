from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Depends
import asyncpg
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DB_URL")

# Manage the database connection pool lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Create the connection pool
    app.state.pool = await asyncpg.create_pool(DB_URL)
    yield
    # Shutdown: Close the connection pool gracefully
    await app.state.pool.close()

# Initialize FastAPI application
app = FastAPI(
    title="Order Book Data API",
    description="High-frequency streaming data and minute rollups for digital assets.",
    version="1.0.0",
    lifespan=lifespan
)

# Dependency to inject a database connection into our routes
async def get_db():
    async with app.state.pool.acquire() as conn:
        yield conn

@app.get("/api/assets", tags=["Metadata"])
async def get_assets(conn: asyncpg.Connection = Depends(get_db)):
    """Retrieves the list of supported trading pairs."""
    records = await conn.fetch("SELECT * FROM assets;")
    return {"assets": [dict(record) for record in records]}

@app.get("/api/metrics/latest", tags=["High-Frequency Data"])
async def get_latest_metrics(
    symbol: str = Query("BTC/USD", description="The trading pair symbol"),
    limit: int = Query(10, le=100, description="Number of microsecond updates to fetch"),
    conn: asyncpg.Connection = Depends(get_db)
):
    """Retrieves the most recent raw tick data for a specific asset."""
    records = await conn.fetch("""
        SELECT timestamp, best_bid, best_ask, spread, mid_price, micro_price, imbalance 
        FROM order_book_metrics 
        WHERE symbol = $1 
        ORDER BY timestamp DESC 
        LIMIT $2;
    """, symbol, limit)
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
        
    return {"symbol": symbol, "data": [dict(record) for record in records]}

@app.get("/api/metrics/historical", tags=["Aggregated Data"])
async def get_historical_metrics(
    symbol: str = Query("BTC/USD", description="The trading pair symbol"),
    limit: int = Query(60, le=1440, description="Number of minute-candles to fetch"),
    conn: asyncpg.Connection = Depends(get_db)
):
    """Retrieves 1-minute aggregated rollups for charting and trend analysis."""
    records = await conn.fetch("""
        SELECT minute_bucket, avg_spread, avg_micro_price, avg_imbalance, tick_count
        FROM minute_rollups 
        WHERE symbol = $1 
        ORDER BY minute_bucket DESC 
        LIMIT $2;
    """, symbol, limit)
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No historical data found for {symbol}")
        
    return {"symbol": symbol, "data": [dict(record) for record in records]}