import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import pika
import json
import threading
import aiohttp
import pandas as pd
import os
from fastapi.staticfiles  import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("token")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UPSTOX_LTP_URL = "https://api.upstox.com/v3/market-quote/ltp?instrument_key="
UNDERLYING_KEYS_FILE = "underlying_keys.txt"



@app.get("/")
async def serve_ui():
    return FileResponse("index11_updated.html")



@app.get("/movers")
async def get_movers():
    """
    Runs top_gainers_top_losers.py in a thread (Windows-compatible).
    asyncio.create_subprocess_exec doesn't work on Windows so we use
    subprocess.run inside a thread executor instead.
    """
    import sys
    import subprocess
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor

    if not os.path.exists("top_gainers_top_losers.py"):
        return {"error": "top_gainers_top_losers.py not found in working directory"}

    print("📡 Running top_gainers_top_losers.py in thread for /movers...")

    # Build asset_symbol -> full company name map
    symbol_to_fullname = {}
    if os.path.exists("companies_only.csv"):
        df = pd.read_csv("companies_only.csv")
        deduped = df.drop_duplicates(subset="asset_symbol")[["asset_symbol", "name"]]
        symbol_to_fullname = dict(zip(deduped["asset_symbol"], deduped["name"]))

    # Run subprocess in a thread — works on Windows unlike asyncio subprocess
    def run_script():
        result = subprocess.run(
            [sys.executable, "top_gainers_top_losers.py"],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = await loop.run_in_executor(pool, run_script)

    if result.returncode != 0:
        print(f"❌ Script error: {result.stderr[:300]}")
        return {"error": f"Script failed: {result.stderr[:200]}"}

    output = result.stdout

    # Parse printed output — format: "SYMBOL  |  2.88%  |  27.30"
    results = []
    for line in output.splitlines():
        line = line.strip()
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        try:
            symbol = parts[0].strip()
            pct = float(parts[1].replace("%", "").strip())
            change = float(parts[2].strip())
            full_name = symbol_to_fullname.get(symbol, symbol)
            results.append({
                "name": full_name,
                "symbol": symbol,
                "pct_change": pct,
                "change": change
            })
        except (ValueError, IndexError):
            continue

    results.sort(key=lambda x: x["pct_change"], reverse=True)
    print(f"✅ /movers: parsed {len(results)} stocks")
    return {"stocks": results, "count": len(results), "failed": 0}


# ---- WebSocket + RabbitMQ bridge (unchanged) ----
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

def start_rabbitmq_consumer(loop):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=600))
    channel = connection.channel()
    channel.queue_declare(queue='insider_alerts', durable=True)

    def callback(ch, method, properties, body):
        message = body.decode()
        asyncio.run_coroutine_threadsafe(manager.broadcast(message), loop)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='insider_alerts', on_message_callback=callback, auto_ack=False)
    print("🚀 Bridge connected to RabbitMQ. Waiting for trades...")
    channel.start_consuming()

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    threading.Thread(target=start_rabbitmq_consumer, args=(loop,), daemon=True).start()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)