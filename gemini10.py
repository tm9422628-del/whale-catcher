import asyncio
import json
import ssl
import websockets
import requests
import os
import pandas as pd
import time
import pika
import duckdb
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
import MarketDataFeedV3_pb2 as pb

load_dotenv()
ACCESS_TOKEN = os.getenv("token")

# --- DATABASE SETUP ---
db_file = f"march_30_v3.duckdb"
db_con = duckdb.connect(db_file)
db_con.execute("""
    CREATE TABLE IF NOT EXISTS march_30_moves (
        timestamp VARCHAR, ticker VARCHAR, strike VARCHAR, 
        option_type VARCHAR, value DOUBLE, ltp DOUBLE, 
        oi BIGINT, category VARCHAR,
        ltp_change DOUBLE, oi_change DOUBLE, volume DOUBLE,
        alert_count INTEGER, time_active_minutes DOUBLE,
        oi_growth_from_open DOUBLE, conviction INTEGER, conviction_label VARCHAR,
        side VARCHAR, acceleration DOUBLE, delta_pct DOUBLE
    )
""")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 900.0  # Master Rolling Window: 15 minutes
BURST_WINDOW = 10.0  # Detection Window: 10 seconds
CHECK_INTERVAL = 0.5
MIN_VAL_THRESHOLD = 50000

# --- OI BASELINE ---
BASELINE_LOCK_TIME = "09:30"

# --- CONVICTION THRESHOLDS ---
CONVICTION_WATCHING    = 30
CONVICTION_SUSPICIOUS  = 60
CONVICTION_HIGH        = 100

# ============================================================
# GLOBAL STATE
# ============================================================
trade_history   = defaultdict(list)
INSTRUMENT_MAP  = {}
last_trade_info = defaultdict(lambda: {"ltt": 0, "vtt": 0, "oi": 0})
data_queue      = asyncio.Queue()
alert_queue     = asyncio.Queue()
oi_baseline     = {}
strike_registry = {}
historical_baseline = {}

def load_historical_baseline():
    global historical_baseline
    today_str = datetime.now().strftime("%d/%m/%Y")
    try:
        result = db_con.execute(f"""
            SELECT ticker, strike, option_type,
                   COUNT(*) as total_alerts,
                   SUM(value) as total_value,
                   COUNT(DISTINCT SUBSTR(timestamp, 1, 10)) as trading_days
            FROM march_30_moves
            WHERE SUBSTR(timestamp, 1, 10) != '{today_str}'
            GROUP BY ticker, strike, option_type
        """).fetchall()
        for row in result:
            ticker, strike, option_type, total_alerts, total_value, trading_days = row
            if trading_days > 0:
                key = f"{ticker}-{strike}-{option_type}"
                historical_baseline[key] = {
                    "avg_alerts_per_day": total_alerts / trading_days,
                    "avg_value_per_day":  total_value  / trading_days,
                    "trading_days":       trading_days
                }
        print(f"📚 Loaded historical baseline for {len(historical_baseline)} strikes", flush=True)
    except Exception as e:
        print(f"⚠️ Historical baseline error: {e}", flush=True)

# ============================================================
# CONVICTION SCORE CALCULATOR
# ============================================================
def compute_conviction(reg_entry, oi_growth, acceleration, delta_pct, hist_key):
    score = 0
    # Signal 1: OI Growth (40 pts)
    score += min(oi_growth * 0.6, 40) if oi_growth > 0 else 0
    # Signal 2: Alert Count (30 pts)
    if reg_entry["alert_count"] >= 10: score += 30
    elif reg_entry["alert_count"] >= 5: score += 15
    # Signal 3: Acceleration (30 pts) - NEW
    if acceleration >= 5.0: score += 30
    elif acceleration >= 3.0: score += 20
    elif acceleration >= 1.5: score += 10
    # Signal 4: Order Flow Imbalance (20 pts) - NEW
    if delta_pct >= 85 or delta_pct <= 15: score += 20
    # Signal 5: History (30 pts)
    hist = historical_baseline.get(hist_key)
    if hist and reg_entry["alert_count"] > (hist["avg_alerts_per_day"] * 2): score += 30

    score = int(min(score, 150))
    label = "HIGH_CONVICTION" if score >= 100 else "SUSPICIOUS" if score >= 60 else "WATCHING" if score >= 30 else "NOISE"
    return score, label

# ============================================================
# RABBITMQ WORKER
# ============================================================
class RabbitMQWorker:
    def __init__(self, host='localhost', queue_name='insider_alerts'):
        self.host, self.queue_name = host, queue_name
        self.connection = self.channel = None

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            return True
        except: return False

    async def run(self):
        if not self.connect(): return
        while True:
            alert_data = await alert_queue.get()
            try:
                db_con.execute("""
                    INSERT INTO march_30_moves VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                """, [
                    alert_data['timestamp'], alert_data['ticker'], str(alert_data['strike']),
                    alert_data['option_type'], alert_data['value'], alert_data['ltp'],
                    int(alert_data['oi']), alert_data['category'], alert_data['ltp_change'],
                    alert_data['oi_change'], alert_data['volume'], alert_data['alert_count'],
                    alert_data['time_active_minutes'], alert_data['oi_growth_from_open'],
                    alert_data['conviction'], alert_data['conviction_label'],
                    alert_data['side'], alert_data['acceleration'], alert_data['delta_pct']
                ])
                if self.channel and not self.channel.is_closed:
                    self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(alert_data))
            except Exception as e:
                print(f"MQ Error: {e}"); self.connect()
            finally: alert_queue.task_done()

mq_worker = RabbitMQWorker()

# ============================================================
# QUEUE WORKER - Market Depth / Side Attribution
# ============================================================
async def queue_worker():
    while True:
        message = await data_queue.get()
        feed_response = pb.FeedResponse()
        try:
            feed_response.ParseFromString(message)
            now = time.time()
            for key, feed in feed_response.feeds.items():
                if not hasattr(feed, 'firstLevelWithGreeks') or not feed.HasField('firstLevelWithGreeks'): continue
                flwg, ltpc = feed.firstLevelWithGreeks, feed.firstLevelWithGreeks.ltpc
                vtt, ltt, price = float(flwg.vtt), int(ltpc.ltt), float(ltpc.ltp)
                current_oi = float(flwg.oi) if hasattr(flwg, 'oi') else last_trade_info[key]["oi"]
                
                # --- AGGRESSOR LOGIC ---
                bid_p, ask_p = float(flwg.firstDepth.bidP), float(flwg.firstDepth.askP)
                mid_p = (bid_p + ask_p) / 2
                side = "BUY" if price >= mid_p else "SELL"

                if ltt > last_trade_info[key]["ltt"] or vtt > last_trade_info[key]["vtt"]:
                    new_qty = (vtt - last_trade_info[key]["vtt"] if last_trade_info[key]["vtt"] > 0 else float(ltpc.ltq))
                    if new_qty > 0:
                        # Store Side info in trade history
                        trade_history[key].append((ltt / 1000, price * new_qty, price, current_oi, new_qty, side))
                    last_trade_info[key].update({"ltt": ltt, "vtt": vtt, "oi": current_oi})
                
                if key not in oi_baseline and current_oi > 0:
                    if datetime.now().strftime("%H:%M") >= BASELINE_LOCK_TIME: oi_baseline[key] = current_oi
        except: pass
        finally: data_queue.task_done()

# ============================================================
# ENERGY MONITOR - Multi-Timeframe Windows
# ============================================================
async def energy_monitor():
    print("⚡ Scanner Active (10s vs 15m)...", flush=True)
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        for key in list(trade_history.keys()):
            # 1. Rolling Memory Trim (15m)
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]: continue

            # 2. Slice: 10-Second Burst
            burst_trades = [t for t in trade_history[key] if now - t[0] <= BURST_WINDOW]
            if not burst_trades: continue
            
            val = sum(t[1] for t in burst_trades)
            if val < MIN_VAL_THRESHOLD: continue

            # 3. Slice: 15-Minute Baseline & Acceleration
            vol_15m = sum(t[4] for t in trade_history[key])
            avg_vol_min_15 = vol_15m / (WINDOW_TIME / 60)
            curr_vol_min = sum(t[4] for t in burst_trades) * (60 / BURST_WINDOW)
            acceleration = curr_vol_min / avg_vol_min_15 if avg_vol_min_15 > 0 else 1.0

            # 4. Aggressor Imbalance
            buys = sum(t[4] for t in burst_trades if t[5] == "BUY")
            sells = sum(t[4] for t in burst_trades if t[5] == "SELL")
            delta_pct = (buys / (buys + sells)) * 100 if (buys + sells) > 0 else 50
            side_label = "BUY_SCRAPE" if buys > sells else "SELL_SCRAPE"

            # 5. Conviction & Alerting
            info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
            reg_key = f"{info['name']}-{info['strike']}-{info['type']}"
            if reg_key not in strike_registry:
                strike_registry[reg_key] = {"first_seen": now, "alert_count": 0, "cumulative_value": 0.0, "alert_timestamps": []}
            
            reg = strike_registry[reg_key]
            reg["alert_count"] += 1
            reg["cumulative_value"] += val
            reg["alert_timestamps"].append(now)
            reg["time_active_minutes"] = (now - reg["first_seen"]) / 60
            
            baseline_oi = oi_baseline.get(key, 0)
            oi_growth = ((burst_trades[-1][3] - baseline_oi) / baseline_oi) * 100 if baseline_oi > 0 else 0
            
            conv, conv_label = compute_conviction(reg, oi_growth, acceleration, delta_pct, reg_key)

            alert_queue.put_nowait({
                "timestamp": datetime.fromtimestamp(burst_trades[-1][0]).strftime('%d/%m/%Y %H:%M:%S'),
                "ticker": info['name'], "strike": str(info['strike']), "option_type": info['type'],
                "value": round(val, 2), "ltp": round(burst_trades[-1][2], 2), "category": side_label,
                "oi": int(burst_trades[-1][3]), "ltp_change": round(burst_trades[-1][2] - burst_trades[0][2], 2),
                "oi_change": round(burst_trades[-1][3] - burst_trades[0][3], 0), "volume": round(sum(t[4] for t in burst_trades), 0),
                "alert_count": reg["alert_count"], "time_active_minutes": round(reg["time_active_minutes"], 1),
                "oi_growth_from_open": round(oi_growth, 1), "conviction": conv, "conviction_label": conv_label,
                "side": side_label, "acceleration": round(acceleration, 1), "delta_pct": round(delta_pct, 1)
            })

def get_market_data_feed_authorize_v3():
    return requests.get("https://api.upstox.com/v3/feed/market-data-feed/authorize", 
                        headers={"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}).json()

async def fetch_market_data(instrument_list):
    asyncio.create_task(queue_worker()); asyncio.create_task(energy_monitor()); asyncio.create_task(mq_worker.run())
    auth_res = get_market_data_feed_authorize_v3()
    async with websockets.connect(auth_res["data"]["authorized_redirect_uri"], ssl=ssl.create_default_context()) as ws:
        await ws.send(json.dumps({"guid": "surge", "method": "sub", "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}}).encode("utf-8"))
        while True: data_queue.put_nowait(await ws.recv())

if __name__ == "__main__":
    load_historical_baseline()
    df = pd.read_csv("atm_option_table.csv")
    keys = [str(x) for x in set(df[df.columns[3:]].values.flatten().tolist()) if str(x) not in ['nan', 'None']]
    print(len(keys))
    INSTRUMENT_MAP = {str(row['ce_instrument_key']): {"name": row['name'], "strike": row['strike_price'], "type": "CE"} for _, row in pd.read_csv("companies_only.csv").iterrows()}
    INSTRUMENT_MAP.update({str(row['pe_instrument_key']): {"name": row['name'], "strike": row['strike_price'], "type": "PE"} for _, row in pd.read_csv("companies_only.csv").iterrows()})
    asyncio.run(fetch_market_data(keys))