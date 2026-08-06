"""
Shared PostgreSQL Schema Definition
-----------------------------------
Both the ingestion engine (tracker.py) and the API server (api.py) call init_schema()
on startup, so either service can come up first against an empty database without the
other having to win a race.
"""

import asyncpg

# Arbitrary but fixed key so concurrent tracker/api startups serialize their DDL.
# Concurrent "CREATE TABLE IF NOT EXISTS" can otherwise collide on pg_type.
_SCHEMA_LOCK_KEY = 8123456789012345


async def init_schema(pool: asyncpg.Pool) -> None:
    """Creates the metrics tables, rollup table, and indexes. Safe to call concurrently."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1);", _SCHEMA_LOCK_KEY)

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

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_order_book_metrics_timestamp
                ON order_book_metrics (timestamp);
            """)

            # Every hot query filters by symbol and orders by timestamp; the
            # timestamp-only index above cannot serve them.
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_order_book_metrics_symbol_timestamp
                ON order_book_metrics (symbol, timestamp DESC);
            """)

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
