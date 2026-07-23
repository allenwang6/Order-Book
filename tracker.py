"""
L2 Order Book High-Frequency Ingestion Engine with Automated Rollups & Storage Cleanup
-------------------------------------------------------------------------------------
Connects to CCXT Pro WebSocket streams (10 depth levels), persists raw ticks, runs a 
background worker for live 1-minute rollups, and purges ticks older than 1 hour.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import ccxt.pro as ccxt
import asyncpg
import os
from dotenv import load_dotenv
import json

load_dotenv()
DB_URL = os.getenv("DB_URL")

async def init_db(pool: asyncpg.Pool) -> None:
    """Initializes the PostgreSQL database schema, indexes, and rollup tables."""
    async with pool.acquire() as conn:
        # Raw ticks table
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
        
        # Index on timestamp for fast time-window lookups and rollups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_order_book_metrics_timestamp 
            ON order_book_metrics (timestamp);
        """)
        
        # Aggregated minute rollups table required for historical charts
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS minute_rollups (
                id SERIAL PRIMARY KEY,
                minute_bucket TIMESTAMP WITH TIME ZONE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                avg_micro_price NUMERIC(18, 8) NOT NULL,
                price_volatility NUMERIC(18, 8) NOT NULL,
                max_spread NUMERIC(18, 8) NOT NULL,
                max_imbalance NUMERIC(8, 4) NOT NULL,
                min_imbalance NUMERIC(8, 4) NOT NULL,
                avg_imbalance NUMERIC(8, 4) NOT NULL,
                CONSTRAINT unique_minute_symbol UNIQUE (minute_bucket, symbol)
            );
        """)

async def watch_and_store(exchange: ccxt.Exchange, symbol: str, pool: asyncpg.Pool) -> None:
    """Listens to real-time L2 order book updates (10 levels), calculates metrics, and persists to DB."""
    try:
        async with pool.acquire() as conn:
            while True:
                order_book = await exchange.watch_order_book(symbol)
                raw_bids = order_book.get('bids', [])
                raw_asks = order_book.get('asks', [])

                if not raw_bids or not raw_asks:
                    continue
                
                # Sliced to 10 levels to conserve Neon storage footprint
                bids = raw_bids[:10]
                asks = raw_asks[:10]

                best_bid_price, best_bid_vol = bids[0][0], bids[0][1]
                best_ask_price, best_ask_vol = asks[0][0], asks[0][1]

                spread = best_ask_price - best_bid_price
                mid_price = (best_ask_price + best_bid_price) / 2.0

                total_top_vol = best_bid_vol + best_ask_vol
                micro_price = (
                    (best_bid_vol * best_ask_price + best_ask_vol * best_bid_price) / total_top_vol
                    if total_top_vol > 0 else mid_price
                )

                sum_bid_vol = sum(bid[1] for bid in bids)
                sum_ask_vol = sum(ask[1] for ask in asks)
                total_depth_vol = sum_bid_vol + sum_ask_vol
                imbalance = (sum_bid_vol - sum_ask_vol) / total_depth_vol if total_depth_vol > 0 else 0.0

                bids_json = json.dumps(bids)
                asks_json = json.dumps(asks)

                await conn.execute("""
                    INSERT INTO order_book_metrics 
                    (symbol, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                """, symbol, best_bid_price, best_ask_price, spread, mid_price, micro_price, imbalance, bids_json, asks_json)

    except Exception as e:
        print(f"WebSocket Error [{symbol}]: {e}")

async def run_minute_rollups(pool: asyncpg.Pool) -> None:
    """Background worker that continuously updates the current minute's metrics in real-time."""
    while True:
        try:
            # Refresh every 5 seconds to keep the current minute's data live on the chart
            await asyncio.sleep(5)

            current_minute_bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            next_bucket = current_minute_bucket + timedelta(minutes=1)

            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO minute_rollups (
                        minute_bucket, symbol, avg_micro_price, price_volatility, 
                        max_spread, max_imbalance, min_imbalance, avg_imbalance
                    )
                    SELECT 
                        $1 AS minute_bucket,
                        symbol,
                        AVG(micro_price) AS avg_micro_price,
                        COALESCE(STDDEV(micro_price), 0) AS price_volatility,
                        MAX(spread) AS max_spread,
                        MAX(imbalance) AS max_imbalance,
                        MIN(imbalance) AS min_imbalance,
                        AVG(imbalance) AS avg_imbalance
                    FROM order_book_metrics
                    WHERE timestamp >= $1 AND timestamp < $2
                    GROUP BY symbol
                    ON CONFLICT (minute_bucket, symbol) 
                    DO UPDATE SET 
                        avg_micro_price = EXCLUDED.avg_micro_price,
                        price_volatility = EXCLUDED.price_volatility,
                        max_spread = EXCLUDED.max_spread,
                        max_imbalance = EXCLUDED.max_imbalance,
                        min_imbalance = EXCLUDED.min_imbalance,
                        avg_imbalance = EXCLUDED.avg_imbalance;
                """, current_minute_bucket, next_bucket)

        except Exception as e:
            print(f"[Rollup Worker Error]: {e}")
            await asyncio.sleep(5)

async def cleanup_old_metrics(pool: asyncpg.Pool) -> None:
    """Background worker that purges raw ticks older than 1 hour to conserve Neon storage."""
    while True:
        try:
            await asyncio.sleep(600)  # Run cleanup every 10 minutes
            
            async with pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM order_book_metrics 
                    WHERE timestamp < NOW() - INTERVAL '1 hour';
                """)
                print(f"[Cleanup Worker] Successfully purged expired raw ticks older than 1 hour.")

        except Exception as e:
            print(f"[Cleanup Worker Error]: {e}")
            await asyncio.sleep(60)

async def main_loop() -> None:
    exchange = ccxt.coinbase()
    symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'AVAX/USD']

    print("Establishing asynchronous connection to PostgreSQL...")
    pool = await asyncpg.create_pool(DB_URL)
    await init_db(pool)
    print("Database connection pool active and schemas verified.")

    print(f"Initializing real-time WebSocket streams, rollup worker, and storage manager...")
    try:
        tasks = [watch_and_store(exchange, symbol, pool) for symbol in symbols]
        tasks.append(run_minute_rollups(pool))
        tasks.append(cleanup_old_metrics(pool))
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\nGracefully shutting down services...")
    finally:
        await exchange.close()
        await pool.close()
        print("All connections closed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting.")