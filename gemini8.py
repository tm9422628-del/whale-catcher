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
db_file = f"march_30_v2.duckdb"
db_con = duckdb.connect(db_file)
db_con.execute("""
    CREATE TABLE IF NOT EXISTS insider_trades (
        timestamp VARCHAR, ticker VARCHAR, strike VARCHAR, 
        option_type VARCHAR, value DOUBLE, ltp DOUBLE, 
        oi BIGINT, category VARCHAR,
        ltp_change DOUBLE, oi_change DOUBLE, volume DOUBLE
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
                db_con.execute("INSERT INTO insider_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                    alert_data['timestamp'], alert_data['ticker'], str(alert_data['strike']),
                    alert_data['option_type'], alert_data['value'], alert_data['ltp'],
                    int(alert_data['oi']), alert_data['category'],
                    alert_data['ltp_change'], alert_data['oi_change'], alert_data['volume']
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
                        # tuple: (timestamp_sec, value, price, oi, qty)
                        # ltt is ms from exchange — divide by 1000 to match time.time() in energy_monitor
                        trade_history[key].append((ltt, price * new_qty, price, current_oi, new_qty))
                        # print(datetime.fromtimestamp(ltt).strftime('%b %d %H:%M:%S')) this line for debugging
                    last_trade_info[key].update({"ltt": ltt, "vtt": vtt, "oi": current_oi})
        except: pass
        finally: data_queue.task_done()








async def energy_monitor():
    print("⚡ Scanner Active...", flush=True)
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        
        # Load the latest ATM table to check for is_atm flags
      

        for key in list(trade_history.keys()):
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]: continue
            
            current_trades = trade_history[key]
            val   = sum(t[1] for t in current_trades)   # total rupee value in window
            total_volume = sum(t[4] for t in current_trades)  # total contracts traded

            # --- PRICE DIRECTION ---
            first_price = current_trades[0][2]
            last_price  = current_trades[-1][2]
            price_change = last_price - first_price  # signed — positive = price went UP

            # Price move as % of starting price — minimum tick is ₹0.05
            # so anything under 0.3% is effectively flat (dead zone)
            price_pct = (price_change / first_price) if first_price > 0 else 0
            PRICE_DEAD_ZONE = 0.003  # 0.3% — below this = price flat

            # --- OI DIRECTION ---
            first_oi = current_trades[0][3]
            last_oi  = current_trades[-1][3]
            oi_change = last_oi - first_oi  # signed — positive = OI increased

            # OI noise filter — need at least 0.5% OI change to call it directional
            OI_DEAD_ZONE = 0.005
            oi_pct = (oi_change / first_oi) if first_oi > 0 else 0
            oi_rising  = oi_pct >  OI_DEAD_ZONE   # new positions being built
            oi_falling = oi_pct < -OI_DEAD_ZONE   # positions being closed
            
            # --- VOLUME WEIGHT ---
            # Average trade size in this window — large avg size = institutional
            avg_trade_size = val / len(current_trades)

            if val >= MIN_VAL_THRESHOLD:
                info = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})

                # --- 4-STATE CLASSIFICATION ---
                # Price flat + OI rising  → fresh positions quietly building = ACCUMULATION
                # Price flat + OI falling → positions quietly exiting       = DISTRIBUTION
                # Price up   + OI rising  → new buyers entering aggressively = LONG_BUILDUP
                # Price down + OI rising  → new sellers writing aggressively = SHORT_BUILDUP
                # Price up   + OI falling → shorts covering/panic buying     = SHORT_COVERING
                # Price down + OI falling → longs exiting/giving up          = LONG_UNWINDING

                if abs(price_pct) <= PRICE_DEAD_ZONE:
                    # Price flat — classify purely on OI
                    if oi_rising:
                        category = "ACCUMULATION"     # quiet buildup, strongest signal
                    elif oi_falling:
                        category = "DISTRIBUTION"     # quiet exit, bearish signal
                    else:
                        category = "ACCUMULATION"     # flat price, flat OI, still notable for value
                else:
                    # Price moved — use combined signal
                    if price_change > 0 and oi_rising:
                        category = "LONG_BUILDUP"     # fresh buyers, price + OI both up
                    elif price_change < 0 and oi_rising:
                        category = "SHORT_BUILDUP"    # fresh sellers, price down OI up
                    elif price_change > 0 and oi_falling:
                        category = "SHORT_COVERING"   # shorts panicking out, explosive but weak
                    elif price_change < 0 and oi_falling:
                        category = "LONG_UNWINDING"   # longs giving up, bearish but weak
                    else:
                        # OI flat but price moved — pure momentum with no position change
                        category = "LONG_BUILDUP" if price_change > 0 else "SHORT_BUILDUP"

                alert_data = {
                    "timestamp":   datetime.fromtimestamp(current_trades[-1][0]/1000).strftime('%b %d %H:%M:%S'),
                    "ticker":      info['name'],
                    "strike":      str(info['strike']),
                    "option_type": info['type'],
                    "value":       round(val, 2),
                    "ltp":         round(last_price, 2),
                    "category":    category,
                    "oi":          int(last_oi),
                    # New fields for frontend display
                    "ltp_change":  round(price_change, 2),    # signed — direction of option price
                    "oi_change":   round(oi_change, 0),        # signed — direction of OI
                    "volume":      round(total_volume, 0),     # total contracts in window
                    "avg_trade_size": round(avg_trade_size, 0) # avg rupee per trade — large = institutional
                }

                alert_queue.put_nowait(alert_data)
                trade_history[key].clear()







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