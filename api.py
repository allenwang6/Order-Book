from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import asyncpg
import asyncio
import os
from dotenv import load_dotenv
from decimal import Decimal

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DB_URL")

# Manage the database connection pool lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DB_URL)
    yield
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

# --- HTML FRONTEND ROUTES ---

@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse("index.html")

@app.get("/charts", include_in_schema=False)
async def serve_charts():
    return FileResponse("charts.html")

# --- WEBSOCKET ROUTE ---

@app.websocket("/ws/stream/{symbol:path}")
async def websocket_stream(websocket: WebSocket, symbol: str, conn: asyncpg.Connection = Depends(get_db)):
    """Pushes real-time order book updates to the frontend via WebSockets."""
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
                # Serialize the datetime
                data['timestamp'] = data['timestamp'].isoformat()
                
                # NEW: Convert any Decimal types to float so JSON can serialize them
                for key, value in data.items():
                    if isinstance(value, Decimal):
                        data[key] = float(value)
                
                await websocket.send_json(data)
                
            await asyncio.sleep(0.1)
            
    except WebSocketDisconnect:
        pass

# --- REST API ROUTES ---

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
    # CHANGED: Added bids and asks to the SELECT query
    records = await conn.fetch("""
        SELECT timestamp, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks 
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
        SELECT *
        FROM minute_rollups 
        WHERE symbol = $1 
        ORDER BY minute_bucket DESC 
        LIMIT $2;
    """, symbol, limit)
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No historical data found for {symbol}")
        
    return {"symbol": symbol, "data": [dict(record) for record in records]}