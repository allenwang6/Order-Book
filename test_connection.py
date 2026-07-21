import psycopg2
import os
from dotenv import load_dotenv

# Load database URL
load_dotenv()

# Database Connection using Neon
DB_URL = os.getenv("DB_URL")

try:
    # psycopg2.connect() establishes connection to cloud database
    with psycopg2.connect(DB_URL) as conn:
        print("Connection established successfully!")

        # A cursor is used to execute SQL commands
        with conn.cursor() as cur:
            cur.execute("Select Version();")
            version = cur.fetchone()
            print(f"Database Version: {version[0]}")

except Exception as e:
    print(f"Connections failed: {e}")
