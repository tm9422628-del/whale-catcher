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

# --- DATABASE SETUP (Today Only) ---
# db_file = f"trades_{datetime.now().strftime('%Y%m%d')}.duckdb"
db_file = f"february_24.duckdb"
db_con = duckdb.connect(db_file)
db_con.execute("""
    CREATE TABLE IF NOT EXISTS insider_trades (
        timestamp VARCHAR, ticker VARCHAR, strike VARCHAR, 
        option_type VARCHAR, value DOUBLE, ltp DOUBLE, 
        oi BIGINT, category VARCHAR
    )
""")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 5.0           
CHECK_INTERVAL = 0.5        
MIN_VAL_THRESHOLD = 50000  

# --- GLOBAL STATE ---
trade_history = defaultdict(list)
INSTRUMENT_MAP = {}
last_trade_info = defaultdict(lambda: {"ltt": 0, "vtt": 0, "oi": 0})
data_queue = asyncio.Queue()
alert_queue = asyncio.Queue()  

class RabbitMQWorker:
    def __init__(self, host='localhost', queue_name='insider_alerts'):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            return True
        except Exception as e:
            print(f"❌ RabbitMQ Connection Error: {e}")
            return False

    async def run(self):
        if not self.connect(): return
        print("📮 Alert Worker (RabbitMQ + DuckDB) started...", flush=True)
        while True:
            alert_data = await alert_queue.get()
            try:
                # 1. Log to DuckDB
                db_con.execute("INSERT INTO insider_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
                    alert_data['timestamp'], alert_data['ticker'], str(alert_data['strike']),
                    alert_data['option_type'], alert_data['value'], alert_data['ltp'],
                    int(alert_data['oi']), alert_data['category']
                ])
                # 2. Publish to RabbitMQ
                if self.channel and not self.channel.is_closed:
                    self.channel.basic_publish(
                        exchange='', routing_key=self.queue_name,
                        body=json.dumps(alert_data),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
            except: self.connect()
            finally: alert_queue.task_done()

mq_worker = RabbitMQWorker()

def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()

# --- THE MISSING FUNCTION FIXED ---
def create_optimized_lookup(active_keys):
    if not os.path.exists("companies_only.csv"): 
        print("❌ companies_only.csv missing!")
        return {}
    master_df = pd.read_csv("companies_only.csv")
    lookup = {}
    active_set = set(active_keys)
    for _, row in master_df.iterrows():
        ce_key, pe_key = str(row['ce_instrument_key']), str(row['pe_instrument_key'])
        if ce_key in active_set: 
            lookup[ce_key] = {"name": row['name'], "strike": row['strike_price'], "type": "CE"}
        if pe_key in active_set: 
            lookup[pe_key] = {"name": row['name'], "strike": row['strike_price'], "type": "PE"}
    return lookup







async def queue_worker():
    while True:
        message = await data_queue.get()
        feed_response = pb.FeedResponse()
        try:
            feed_response.ParseFromString(message)
            now = time.time()
            for key, feed in feed_response.feeds.items():
                if not hasattr(feed, 'firstLevelWithGreeks') or not feed.HasField('firstLevelWithGreeks'): continue
                flwg = feed.firstLevelWithGreeks
                ltpc = flwg.ltpc
                vtt, ltt, price = float(flwg.vtt), int(ltpc.ltt), float(ltpc.ltp)
                current_oi = float(flwg.oi) if hasattr(flwg, 'oi') else last_trade_info[key]["oi"]

                if ltt > last_trade_info[key]["ltt"] or vtt > last_trade_info[key]["vtt"]:
                    new_qty = vtt - last_trade_info[key]["vtt"] if last_trade_info[key]["vtt"] > 0 else float(ltpc.ltq)
                    if new_qty > 0:
                        trade_history[key].append((now, price * new_qty, price, current_oi))
                    last_trade_info[key].update({"ltt": ltt, "vtt": vtt, "oi": current_oi})
        except: pass
        finally: data_queue.task_done()








async def energy_monitor():
    print("⚡ Scanner Active...", flush=True)
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        
        # Load the latest ATM table to check for is_atm flags
        try:
            df_atm = pd.read_csv("atm_option_table.csv")
        except:
            df_atm = pd.DataFrame()

        for key in list(trade_history.keys()):
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]: continue
            
            current_trades = trade_history[key]
            val = sum(t[1] for t in current_trades)
            change = current_trades[-1][2] - current_trades[0][2] # Price change in window
            
            if val >= MIN_VAL_THRESHOLD:
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
                
                # 1. Restore the Category Logic
                category = "HIDDEN_ACCUMULATION" if abs(change) < 0.02 else "MOMENTUM"
                
                # 2. Determine if this is an ATM instrument
                is_atm = False
                if not df_atm.empty:
                    stock_row = df_atm[df_atm['name'] == info['name']]
                    if not stock_row.empty:
                        atm_keys = [str(stock_row.iloc[0]['atm_ce']), str(stock_row.iloc[0]['atm_pe'])]
                        if str(key) in atm_keys:
                            is_atm = True

                # 3. Prepare the full alert data
                alert_data = {
                    "timestamp": time.strftime('%H:%M:%S'), 
                    "ticker": info['name'], 
                    "strike": str(info['strike']), 
                    "option_type": info['type'],
                    "value": round(val, 2), 
                    "ltp": current_trades[-1][2],
                    "category": category, 
                    "oi": current_trades[-1][3],
                    "is_atm": is_atm
                }
                
                alert_queue.put_nowait(alert_data)
                trade_history[key].clear() # Reset after alert to prevent double triggers







async def fetch_market_data(instrument_list):
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = ssl_ctx.verify_mode = False
    asyncio.create_task(queue_worker())
    asyncio.create_task(energy_monitor())
    asyncio.create_task(mq_worker.run())
    while True:
        try:
            auth_res = get_market_data_feed_authorize_v3()
            async with websockets.connect(auth_res["data"]["authorized_redirect_uri"], ssl=ssl_ctx) as ws:
                sub_msg = {"guid": "surge", "method": "sub", "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}}
                await ws.send(json.dumps(sub_msg).encode("utf-8"))
                while True: data_queue.put_nowait(await ws.recv())
        except: await asyncio.sleep(5)

if __name__ == "__main__":
    print("starting on ", time.strftime('%b %d %H:%M:%S'))
    if not os.path.exists("atm_option_table.csv"):
        print("❌ Error: atm_option_table.csv not found! Run the generator script first.")
    else:
        df = pd.read_csv("atm_option_table.csv")
        
        # 1. Skip the first 3 columns (name, underlying_key, spot_price)
        # 2. Grab every instrument key from the remaining columns (atm_ce, atm_pe, plus_4_ce, etc.)
        print(df.columns)
        instrument_cols = df.columns[3:] 
        
        # Flatten all these columns into a single list of unique keys
        keys = [str(x) for x in set(df[instrument_cols].values.flatten().tolist()) 
                if str(x) != 'nan' and str(x) != 'None']
        print(len(keys))

        if keys:
            print(f"✅ Subscribing to {len(keys)} unique instruments from {len(df)} stocks...")
            INSTRUMENT_MAP = create_optimized_lookup(keys)
            asyncio.run(fetch_market_data(keys))
        else:
            print("❌ No valid instrument keys found in atm_option_table.csv")