"""
FastAPI Microstructure Data Server
----------------------------------
Serves real-time WebSocket streaming feeds, raw tick endpoints, and historical metrics.

Streaming is fanned out from a single background poller rather than polled per client, so
database load stays constant regardless of how many dashboards are open and no WebSocket
ever holds a pooled connection.
"""

from collections import defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import asyncpg
import asyncio
import logging
import os
from dotenv import load_dotenv
from decimal import Decimal

from db import init_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z"
)
logger = logging.getLogger("OrderBookAPI")

load_dotenv()
DB_URL = os.getenv("DB_URL")

POLL_INTERVAL = 0.1
# Bounded so a stalled client cannot grow its queue without limit; the poller drops the
# oldest tick instead of blocking the fan-out for everyone else.
CLIENT_QUEUE_SIZE = 100

TICK_COLUMNS = "timestamp, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the connection pool, schema, and tick broadcaster across application states."""
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)

    # The API can be started before (or without) the tracker, so it creates the schema
    # itself rather than serving 500s until the tracker gets there first.
    await init_schema(app.state.pool)

    app.state.subscribers = defaultdict(set)
    app.state.poller = asyncio.create_task(broadcast_ticks(app))

    yield

    app.state.poller.cancel()
    try:
        await app.state.poller
    except asyncio.CancelledError:
        pass
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

def serialize_record(record: asyncpg.Record) -> dict:
    """Formats timestamps and converts Decimals to floats for JSON serialization."""
    data = dict(record)
    data['timestamp'] = data['timestamp'].isoformat()
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = float(value)
    return data

# --- HTML FRONTEND ROUTES ---

@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse("index.html")

@app.get("/charts", include_in_schema=False)
async def serve_charts():
    return FileResponse("charts.html")

@app.get("/api/rollups")
async def get_rollups(symbol: str = Query(...)):
    """Fetches full 1-minute aggregated rollups for charts, tables, and summary cards."""
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                minute_bucket,
                avg_micro_price,
                price_volatility,
                max_spread,
                max_imbalance,
                min_imbalance,
                avg_imbalance
            FROM minute_rollups
            WHERE symbol = $1
            ORDER BY minute_bucket DESC
            LIMIT 60;
        """, symbol)
        return [dict(row) for row in reversed(rows)]

# --- WEBSOCKET STREAMING ROUTE ---

async def broadcast_ticks(app: FastAPI) -> None:
    """Single poller that fetches the newest tick for every subscribed symbol and fans it
    out to that symbol's listeners. One query per cycle total, not one per client."""
    last_timestamps = {}

    while True:
        try:
            symbols = [sym for sym, queues in app.state.subscribers.items() if queues]

            if symbols:
                async with app.state.pool.acquire() as conn:
                    records = await conn.fetch(f"""
                        SELECT DISTINCT ON (symbol) symbol, {TICK_COLUMNS}
                        FROM order_book_metrics
                        WHERE symbol = ANY($1::text[])
                        ORDER BY symbol, timestamp DESC;
                    """, symbols)

                for record in records:
                    symbol = record['symbol']
                    if record['timestamp'] == last_timestamps.get(symbol):
                        continue

                    last_timestamps[symbol] = record['timestamp']
                    payload = serialize_record(record)
                    payload.pop('symbol', None)

                    for queue in list(app.state.subscribers.get(symbol, ())):
                        if queue.full():
                            # Drop the oldest rather than stall the fan-out on a slow client.
                            try:
                                queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                        queue.put_nowait(payload)

                # Forget symbols nobody is watching so the dict cannot grow unbounded.
                for symbol in list(last_timestamps):
                    if symbol not in app.state.subscribers:
                        del last_timestamps[symbol]

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick broadcaster error: {e}")

        await asyncio.sleep(POLL_INTERVAL)

async def _forward_ticks(websocket: WebSocket, queue: asyncio.Queue) -> None:
    """Pushes queued ticks to the client until the socket closes."""
    while True:
        payload = await queue.get()
        await websocket.send_json(payload)

async def _await_disconnect(websocket: WebSocket) -> None:
    """Reads from the socket purely to observe the client going away.

    Without this, a disconnect is only noticed as a side effect of a failed send, so an
    idle feed would leave the subscription (and its task) alive indefinitely.
    """
    while True:
        message = await websocket.receive()
        if message.get("type") == "websocket.disconnect":
            return

@app.websocket("/ws/stream/{symbol:path}")
async def websocket_stream(websocket: WebSocket, symbol: str):
    """Pushes real-time 10-level order book ticks and metrics to the client interface."""
    await websocket.accept()

    queue: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE_SIZE)
    websocket.app.state.subscribers[symbol].add(queue)

    forwarder = asyncio.create_task(_forward_ticks(websocket, queue))
    watcher = asyncio.create_task(_await_disconnect(websocket))

    try:
        done, pending = await asyncio.wait(
            {forwarder, watcher}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        # Consume results so a send failing on an already-closed socket does not
        # surface as an unretrieved task exception.
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                logger.warning(f"WebSocket stream error [{symbol}]: {exc}")
    finally:
        subscribers = websocket.app.state.subscribers
        subscribers[symbol].discard(queue)
        if not subscribers[symbol]:
            del subscribers[symbol]

# --- REST API ENDPOINTS ---

@app.get("/api/metrics/latest", tags=["High-Frequency Data"])
async def get_latest_metrics(
    symbol: str = Query("BTC/USD", description="Trading pair symbol"),
    limit: int = Query(15, ge=1, le=100, description="Number of tick records to retrieve"),
    conn: asyncpg.Connection = Depends(get_db)
):
    """Retrieves the most recent raw ticks including 10-level depth arrays."""
    records = await conn.fetch(f"""
        SELECT {TICK_COLUMNS}
        FROM order_book_metrics
        WHERE symbol = $1
        ORDER BY timestamp DESC
        LIMIT $2;
    """, symbol, limit)

    if not records:
        raise HTTPException(status_code=404, detail=f"No data found for symbol: {symbol}")

    return {"symbol": symbol, "data": [dict(record) for record in records]}
