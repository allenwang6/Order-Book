# order book pipeline
An asynchronous market data pipeline that ingests real-time cryptocurrency order book depth, calculates microstructure metrics, and serves them to an interactive streaming dashboard. 

# system details
The system uses CCXT to maintain WebSocket connections to Coinbase. It parses raw L2 depth arrays to calculate volume-weighted micro-price and order book imbalance (OBI) in real-time. 

These metrics are inserted into a PostgreSQL database. Background workers handle 1-minute data rollups natively within the database to prevent clock drift and automatically prune old raw ticks to manage storage. A FastAPI backend streams the live ticks via WebSockets and serves the historical rollups to the frontend UI.

# setup environment
The easiest way to run the project is using Docker. Create a `.env` file in the project root directory:
```
DB_URL=postgresql://admin:secretpassword@db:5432/orderbook
```

# running the program
```
docker compose up --build
```
Running this command will start the PostgreSQL database, launch the data ingestion tracker, and host the web dashboard on http://localhost:8000. 

Because the historical 1-minute rollup chart requires live depth data that cannot be fetched retroactively, the chart will be empty on the first run. The database will populate naturally as the program runs. The data is stored in a persistent Docker volume and will remain saved across future restarts.

# manual development setup
If you want to run the project without Docker, you can set up a local virtual environment:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

You will need a running PostgreSQL database. Update the `.env` file with your specific database URL, then run the backend and the ingestion engine concurrently:

```
python tracker.py
```
```
uvicorn api:app --port 8000
```