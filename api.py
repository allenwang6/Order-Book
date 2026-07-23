"""
FastAPI Microstructure Data Server
----------------------------------
Serves real-time WebSocket streaming feeds, raw tick endpoints, and historical metrics.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import asyncpg
import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()
DB_URL = os.getenv("DB_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the connection pool lifecycle across application states."""
    app.state.pool = await asyncpg.create_pool(DB_URL)
    yield
    await app.state.pool.close()

app = FastAPI(
    title="Order Book Microstructure API",
    description="High-frequency streaming data and minute rollups for cryptocurrency pairs.",
    version="2.0.0",
    lifespan=lifespan
)

async def get_db():
    """Dependency injection for database connections."""
    async with app.state.pool.acquire() as conn:
        yield conn

# --- HTML FRONTEND ROUTES ---

@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse("index.html")

@app.get("/charts", include_in_schema=False)
async def serve_charts():
    return FileResponse("charts.html")

# --- WEBSOCKET STREAMING ROUTE ---

@app.websocket("/ws/stream/{symbol:path}")
async def websocket_stream(websocket: WebSocket, symbol: str, conn: asyncpg.Connection = Depends(get_db)):
    """Pushes real-time 20-level order book ticks and metrics to the client interface."""
    await websocket.accept()
    last_timestamp = None
    
    try:
        while True:
            record = await conn.fetchrow("""
                SELECT timestamp, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks
                FROM order_book_metrics 
                WHERE symbol = $1 
                ORDER BY timestamp DESC 
                LIMIT 1;
            """, symbol)
            
            if record and record['timestamp'] != last_timestamp:
                last_timestamp = record['timestamp']
                data = dict(record)
                
                # Format timestamps and convert Decimals to floats for JSON serialization
                data['timestamp'] = data['timestamp'].isoformat()
                for key, value in data.items():
                    if isinstance(value, Decimal):
                        data[key] = float(value)
                
                await websocket.send_json(data)
                
            await asyncio.sleep(0.1)
            
    except WebSocketDisconnect:
        pass

# --- REST API ENDPOINTS ---

@app.get("/api/metrics/latest", tags=["High-Frequency Data"])
async def get_latest_metrics(
    symbol: str = Query("BTC/USD", description="Trading pair symbol"),
    limit: int = Query(15, le=100, description="Number of tick records to retrieve"),
    conn: asyncpg.Connection = Depends(get_db)
):
    """Retrieves the most recent raw ticks including 20-level depth arrays."""
    records = await conn.fetch("""
        SELECT timestamp, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks 
        FROM order_book_metrics 
        WHERE symbol = $1 
        ORDER BY timestamp DESC 
        LIMIT $2;
    """, symbol, limit)
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for symbol: {symbol}")
        
    return {"symbol": symbol, "data": [dict(record) for record in records]}

@app.get("/api/metrics/historical", tags=["Aggregated Data"])
async def get_historical_metrics(
    symbol: str = Query("BTC/USD", description="Trading pair symbol"),
    limit: int = Query(60, le=1440, description="Number of minute candles"),
    conn: asyncpg.Connection = Depends(get_db)
):
    """Retrieves 1-minute aggregated rollups for trend visualization."""
    records = await conn.fetch("""
        SELECT *
        FROM minute_rollups 
        WHERE symbol = $1 
        ORDER BY minute_bucket DESC 
        LIMIT $2;
    """, symbol, limit)
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No historical data found for symbol: {symbol}")
        
    return {"symbol": symbol, "data": [dict(record) for record in records]}