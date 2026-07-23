import asyncio
from datetime import datetime
import ccxt.pro as ccxt
import asyncpg
import os
from dotenv import load_dotenv
import json

# Load database URL
load_dotenv()

# Database Connection using Neon
DB_URL = os.getenv("DB_URL")

async def init_db(pool):
    """Creates the metrics table if it doesn't exist yet"""
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
                bids JSONB,  -- Added to support Market Depth Chart
                asks JSONB   -- Added to support Market Depth Chart
                );
        """)

async def watch_and_store(exchange, symbol, pool):
    """Listens to the WebSocket Stream from coinbase for a symbol"""
    try:
        # Acquire a single, dedicated connection for this specific asset's high-frequency stream
        async with pool.acquire() as conn:
            while True:
                # Fetch Level 2 Order Book (Top 5 Levels)
                order_book = await exchange.watch_order_book(symbol)

                bids = order_book.get('bids', [])
                asks = order_book.get('asks', [])

                if not bids or not asks:
                    continue
                
                # Top of book values
                best_bid_price, best_bid_vol = bids[0][0], bids[0][1]
                best_ask_price, best_ask_vol = asks[0][0], asks[0][1]

                # Calculate standard metrics
                spread = best_ask_price - best_bid_price
                mid_price = (best_ask_price + best_bid_price) / 2.0

                # Calculate micro price
                total_top_vol = best_bid_vol + best_ask_vol
                micro_price = (best_bid_vol * best_ask_price + best_ask_vol * best_bid_price) / total_top_vol

                # Calculate order book imbalance across 5 levels
                sum_bid_vol = sum(bid[1] for bid in bids)
                sum_ask_vol = sum(ask[1] for ask in asks)
                total_depth_vol = sum_bid_vol + sum_ask_vol
                imbalance = (sum_bid_vol - sum_ask_vol) / total_depth_vol if total_depth_vol > 0 else 0

                # Grab top 5 price levels and convert to JSON string
                bids_json = json.dumps(order_book['bids'][:5])
                asks_json = json.dumps(order_book['asks'][:5])

                # Write to database using the dedicated connection (conn.execute)
                await conn.execute("""
                    INSERT INTO order_book_metrics (symbol, best_bid, best_ask, spread, mid_price, micro_price, imbalance, bids, asks)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                """, symbol, best_bid_price, best_ask_price, spread, mid_price, micro_price, imbalance, bids_json, asks_json)

                # Terminal feedback
                time_str = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                print(f"[{time_str}] {symbol} | Spread: {spread:.2f} | Mid: {mid_price:.2f} | Micro: {micro_price:.2f} | OBI: {imbalance:+.3f}")

    except Exception as e:
        print(f"Websocket Error for {symbol}: {e}")

async def main_loop():
    # Public exchange setup using coinbase 
    exchange = ccxt.coinbase()
    symbols = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'AVAX/USD']

    print("Connecting to PostgreSQL...")
    pool = await asyncpg.create_pool(DB_URL)
    await init_db(pool)
    print("Database table ready")

    print(f"Opening WebSocket streams for {len(symbols)} assets...")
    try:
        tasks = [watch_and_store(exchange, symbol, pool) for symbol in symbols]
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        print("\nStopping listeners...")
    finally:
        await exchange.close()
        await pool.close()
        print("Connections closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass