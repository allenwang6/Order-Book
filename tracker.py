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
                mid_price NUMERIC(18, 8) NOT NULL
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
        best_bid = bids[0][0]
        best_ask = asks[0][0]

        # Calculate core metrics
        spread = best_ask - best_bid
        mid_price = (best_ask + best_bid) / 2.0

        # Write to database
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO order_book_metrics (symbol, best_bid, best_ask, spread, mid_price)
                VALUES (%s, %s, %s, %s, %s);
            """, (symbol, best_bid, best_ask, spread, mid_price))
            conn.commit()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved {symbol} | Bid: {best_bid:.2f} | Ask: {best_ask:.2f} | Spread: {spread:.4f}")

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