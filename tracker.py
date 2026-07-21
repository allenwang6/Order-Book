import time
from datetime import datetime
import ccxt
import psycopg2
import os
from dotenv import load_dotenv

# Load database URL
load_dotenv()

# Database Connection using Neon
DB_URL = os.getenv("DB_URL")

def init_db(conn):
    """Creates the metrics table if it doesn't exist yet"""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_book_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                symbol VARCHAR(20) NOT NULL,
                best_bid NUMERIC(18, 8) NOT NULL,
                best_ask NUMERIC(18, 8) NOT NULL,
                spread NUMERIC(18, 8) NOT NULL,
                mid_price NUMERIC(18, 8) NOT NULL,
                micro_price NUMERIC(18, 8) NOT NULL,
                imbalance NUMERIC(8, 4) NOT NULL
                );
        """)
        conn.commit()

def fetch_and_store(exchange, symbol, conn):
    """Fetches top-of-book data, computes metrics, and inserts into PostgreSQL"""
    try:
        # Fetch Level 2 Order Book (Top 5 Levels)
        order_book = exchange.fetch_order_book(symbol, limit=5)

        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])

        if not bids or not asks:
            print("Recieved empty order book response.")
            return
        
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

        # Write to database
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO order_book_metrics (symbol, best_bid, best_ask, spread, mid_price, micro_price, imbalance)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (symbol, best_bid_price, best_ask_price, spread, mid_price, micro_price, imbalance))
            conn.commit()

        # Terminal feedback with new metrics
        time_str = datetime.now().strftime('%H:%M:%S')
        print(f"[{time_str}] {symbol} | Spread: {spread:.2f} | Mid: {mid_price:.2f} | Micro: {micro_price:.2f} | OBI: {imbalance:+.3f}")

    except Exception as e:
        print(f"Error during execution: {e}")

def main():
    # Public exchange setup using coinbase 
    exchange = ccxt.coinbase()
    symbol = 'BTC/USD'

    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(DB_URL)

    # Initialize database
    init_db(conn)
    print("Database table ready")

    print(f"Starting order book logger for {symbol} (Polling every 5 seconds)")
    try:
        while True:
            fetch_and_store(exchange, symbol, conn)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping logger...")
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()