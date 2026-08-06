"""
L2 Order Book High-Frequency Ingestion Engine
---------------------------------------------
Features:
- CCXT Pro WebSocket streams (10 depth levels) with exponential backoff reconnection.
- Production-grade structured logging (ISO timestamps, log levels).
- Batched database writes so feed ingestion never blocks on a round trip.
- Background worker for live 1-minute rollups.
- Background storage manager purging raw ticks (1h) and minute rollups (24h).
"""

import asyncio
import ccxt.pro as ccxt
import asyncpg
import os
from dotenv import load_dotenv
import json
import logging

from db import init_schema

# Configure production-grade structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z"
)
logger = logging.getLogger("OrderBookTracker")

load_dotenv()
DB_URL = os.getenv("DB_URL")

DEPTH_LEVELS = 10

# Ingestion buffer. Bounded so a database outage cannot exhaust memory; the oldest
# ticks are dropped first, since stale depth is worthless in a live feed.
TICK_BUFFER_SIZE = 10_000
BATCH_MAX_ROWS = 500

INSERT_TICK_SQL = """
    INSERT INTO order_book_metrics
    (symbol, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
"""


def _rollup_sql(window: str) -> str:
    """Builds the rollup upsert for a fixed, code-controlled lookback window.

    Rows are bucketed by their own timestamp rather than by NOW(), and the window always
    reaches back past the current minute boundary, so the trailing seconds of a minute are
    included and a skipped cycle is repaired on the next pass.
    """
    return f"""
        INSERT INTO minute_rollups (
            minute_bucket, symbol, avg_micro_price, price_volatility,
            max_spread, max_imbalance, min_imbalance, avg_imbalance
        )
        SELECT
            date_trunc('minute', timestamp) AS minute_bucket,
            symbol,
            AVG(micro_price) AS avg_micro_price,
            COALESCE(STDDEV(micro_price), 0) AS price_volatility,
            MAX(spread) AS max_spread,
            MAX(imbalance) AS max_imbalance,
            MIN(imbalance) AS min_imbalance,
            AVG(imbalance) AS avg_imbalance
        FROM order_book_metrics
        WHERE timestamp >= date_trunc('minute', NOW()) - INTERVAL '{window}'
        GROUP BY 1, 2
        ON CONFLICT (minute_bucket, symbol)
        DO UPDATE SET
            avg_micro_price = EXCLUDED.avg_micro_price,
            price_volatility = EXCLUDED.price_volatility,
            max_spread = EXCLUDED.max_spread,
            max_imbalance = EXCLUDED.max_imbalance,
            min_imbalance = EXCLUDED.min_imbalance,
            avg_imbalance = EXCLUDED.avg_imbalance;
    """

# Covers the current minute plus the one before it, so the previous minute keeps being
# re-upserted until it is genuinely complete.
ROLLUP_LIVE_SQL = _rollup_sql("1 minute")
# One pass at startup over the raw retention window, so a restart does not leave
# permanent holes wherever raw ticks still exist.
ROLLUP_BACKFILL_SQL = _rollup_sql("1 hour")


async def watch_and_store(exchange: ccxt.Exchange, symbol: str, queue: asyncio.Queue) -> None:
    """Listens to real-time L2 order book updates with exponential backoff reconnection logic.

    Computed ticks are handed to the shared write buffer; this loop never touches the
    database, so ingestion speed is not capped by write latency.
    """
    backoff = 2
    max_backoff = 30
    dropped = 0

    while True:
        try:
            logger.info(f"Connecting to WebSocket feed for {symbol}...")
            while True:
                order_book = await exchange.watch_order_book(symbol)

                # Reset backoff upon successful message receipt
                backoff = 2

                raw_bids = order_book.get('bids', [])
                raw_asks = order_book.get('asks', [])

                if not raw_bids or not raw_asks:
                    continue

                bids = raw_bids[:DEPTH_LEVELS]
                asks = raw_asks[:DEPTH_LEVELS]

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

                row = (
                    symbol, best_bid_price, best_ask_price, spread, mid_price,
                    micro_price, imbalance, json.dumps(bids), json.dumps(asks)
                )

                if queue.full():
                    try:
                        queue.get_nowait()
                        dropped += 1
                        if dropped % 1000 == 1:
                            logger.warning(
                                f"Write buffer saturated [{symbol}]: dropped {dropped} ticks "
                                f"so far. The database is not keeping up with the feed."
                            )
                    except asyncio.QueueEmpty:
                        pass

                queue.put_nowait(row)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"WebSocket Error [{symbol}]: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

async def write_ticks(pool: asyncpg.Pool, queue: asyncio.Queue) -> None:
    """Drains the shared buffer and writes ticks in batches.

    Blocks for the first row, then takes whatever else has already arrived, so latency
    stays low when the feed is quiet and batches grow automatically when it is busy.
    """
    while True:
        try:
            batch = [await queue.get()]
            while len(batch) < BATCH_MAX_ROWS:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            async with pool.acquire() as conn:
                await conn.executemany(INSERT_TICK_SQL, batch)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick Writer Error: {e}. Retrying in 2s...")
            await asyncio.sleep(2)

async def run_minute_rollups(pool: asyncpg.Pool) -> None:
    """Background worker that continuously updates recent minute metrics using database time."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(ROLLUP_BACKFILL_SQL)
        logger.info("Backfilled minute rollups from available raw tick history.")
    except Exception as e:
        logger.error(f"Rollup Backfill Error: {e}")

    while True:
        try:
            await asyncio.sleep(5)

            async with pool.acquire() as conn:
                await conn.execute(ROLLUP_LIVE_SQL)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Rollup Worker Error: {e}")
            await asyncio.sleep(5)

async def cleanup_old_metrics(pool: asyncpg.Pool) -> None:
    """Background worker that purges raw ticks and rollups to conserve Neon storage."""
    while True:
        try:
            await asyncio.sleep(600)

            async with pool.acquire() as conn:
                # Purge raw ticks older than 1 hour
                await conn.execute("""
                    DELETE FROM order_book_metrics
                    WHERE timestamp < NOW() - INTERVAL '1 hour';
                """)

                # Purge aggregated minute rollups older than 24 hours
                await conn.execute("""
                    DELETE FROM minute_rollups
                    WHERE minute_bucket < NOW() - INTERVAL '24 hours';
                """)

                logger.info("Successfully purged expired raw ticks (>1h) and old minute rollups (>24h).")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Cleanup Worker Error: {e}")
            await asyncio.sleep(60)

async def main_loop() -> None:
    exchange = ccxt.coinbase()
    symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'AVAX/USD']

    logger.info("Establishing asynchronous connection to PostgreSQL...")
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    await init_schema(pool)
    logger.info("Database connection pool active and schemas verified.")

    tick_queue: asyncio.Queue = asyncio.Queue(maxsize=TICK_BUFFER_SIZE)

    logger.info("Initializing resilient real-time WebSocket streams, rollup worker, and storage manager...")
    try:
        tasks = [watch_and_store(exchange, symbol, tick_queue) for symbol in symbols]
        tasks.append(write_ticks(pool, tick_queue))
        tasks.append(run_minute_rollups(pool))
        tasks.append(cleanup_old_metrics(pool))
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Gracefully shutting down services...")
    finally:
        await exchange.close()
        await pool.close()
        logger.info("All connections closed successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
