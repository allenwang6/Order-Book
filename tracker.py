"""
L2 Order Book High-Frequency Ingestion Engine
---------------------------------------------
Connects to CCXT Pro WebSocket streams, computes real-time microstructural 
metrics across 20 depth levels, and asynchronously streams ticks into PostgreSQL.
"""

import asyncio
from datetime import datetime
import ccxt.pro as ccxt
import asyncpg
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DB_URL")

async def init_db(pool: asyncpg.Pool) -> None:
    """Initializes the PostgreSQL database schema if tables do not exist."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS order_book_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                symbol VARCHAR(20) NOT NULL,
                best_bid NUMERIC(18, 8) NOT NULL,
                best_ask NUMERIC(18, 8) NOT NULL,
                spread NUMERIC(18, 8) NOT NULL,
                mid_price NUMERIC(18, 8) NOT NULL,
                micro_price NUMERIC(18, 8) NOT NULL,
                imbalance NUMERIC(8, 4) NOT NULL,
                bids JSONB,
                asks JSONB
            );
        """)

async def watch_and_store(exchange: ccxt.Exchange, symbol: str, pool: asyncpg.Pool) -> None:
    """Listens to real-time L2 order book updates, calculates metrics, and persists to DB."""
    try:
        # Acquire a dedicated connection for high-frequency pipelining
        async with pool.acquire() as conn:
            while True:
                # Fetch Level 2 Order Book (Top 20 Levels for deep liquidity profile)
                order_book = await exchange.watch_order_book(symbol)

                raw_bids = order_book.get('bids', [])
                raw_asks = order_book.get('asks', [])

                if not raw_bids or not raw_asks:
                    continue
                
                # Slice top 20 levels for deep liquidity analysis
                bids = raw_bids[:20]
                asks = raw_asks[:20]

                # Top of book values
                best_bid_price, best_bid_vol = bids[0][0], bids[0][1]
                best_ask_price, best_ask_vol = asks[0][0], asks[0][1]

                # Core microstructure calculations
                spread = best_ask_price - best_bid_price
                mid_price = (best_ask_price + best_bid_price) / 2.0

                # Micro price (Volume-weighted top-of-book price)
                total_top_vol = best_bid_vol + best_ask_vol
                micro_price = (
                    (best_bid_vol * best_ask_price + best_ask_vol * best_bid_price) / total_top_vol
                    if total_top_vol > 0 else mid_price
                )

                # Order Book Imbalance (OBI) across 20 depth levels
                sum_bid_vol = sum(bid[1] for bid in bids)
                sum_ask_vol = sum(ask[1] for ask in asks)
                total_depth_vol = sum_bid_vol + sum_ask_vol
                imbalance = (sum_bid_vol - sum_ask_vol) / total_depth_vol if total_depth_vol > 0 else 0.0

                # Serialize depth arrays to JSONB strings
                bids_json = json.dumps(bids)
                asks_json = json.dumps(asks)

                # Execute database write using the dedicated connection pipe
                await conn.execute("""
                    INSERT INTO order_book_metrics 
                    (symbol, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                """, symbol, best_bid_price, best_ask_price, spread, mid_price, micro_price, imbalance, bids_json, asks_json)

                # Console feedback
                time_str = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                print(f"[{time_str}] {symbol:<8} | Spread: {spread:>8.4f} | Mid: {mid_price:>10.2f} | OBI: {imbalance:>+.3f}")

    except Exception as e:
        print(f"WebSocket Error [{symbol}]: {e}")

async def main_loop() -> None:
    exchange = ccxt.coinbase()
    symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'AVAX/USD']

    print("Establishing asynchronous connection to PostgreSQL...")
    pool = await asyncpg.create_pool(DB_URL)
    await init_db(pool)
    print("Database connection pool active and schema verified.")

    print(f"Initializing real-time WebSocket streams for {len(symbols)} assets...")
    try:
        tasks = [watch_and_store(exchange, symbol, pool) for symbol in symbols]
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\nGracefully shutting down WebSocket streams...")
    finally:
        await exchange.close()
        await pool.close()
        print("All connections closed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting.")