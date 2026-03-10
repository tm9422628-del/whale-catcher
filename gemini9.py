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
        oi_growth_from_open DOUBLE, conviction INTEGER, conviction_label VARCHAR
    )
""")

# --- ENERGY SETTINGS ---
WINDOW_TIME = 5.0
CHECK_INTERVAL = 0.5
MIN_VAL_THRESHOLD = 50000

# --- OI BASELINE: wait until 9:30 before locking baseline ---
# First OI reading after BASELINE_LOCK_TIME is used as the day's baseline
BASELINE_LOCK_TIME = "09:30"   # HH:MM — change to "09:15" if you want immediate

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

# -- OI baseline: locked once per instrument after 9:30 --
oi_baseline = {}   # instrument_key -> float (OI at ~9:30)

# -- Strike registry: persistent memory for the day --
# key: "TICKER-STRIKE-TYPE"  e.g. "INFOSYS LIMITED-1600-CE"
# value: dict with running stats
strike_registry = {}

# -- Historical baseline loaded from DuckDB at startup --
# key: "TICKER-STRIKE-TYPE"
# value: {"avg_alerts_per_day": float, "avg_value_per_day": float}
historical_baseline = {}


# ============================================================
# LOAD HISTORICAL BASELINE FROM DUCKDB
# ============================================================
def load_historical_baseline():
    """
    At startup, read all past data from DuckDB (not today) and compute
    per-strike average daily alert count and average daily value.
    This becomes the 'normal' baseline to compare today against.
    """
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
            if trading_days and trading_days > 0:
                key = f"{ticker}-{strike}-{option_type}"
                historical_baseline[key] = {
                    "avg_alerts_per_day": total_alerts / trading_days,
                    "avg_value_per_day":  total_value  / trading_days,
                    "trading_days":       trading_days
                }

        print(f"📚 Loaded historical baseline for {len(historical_baseline)} strike combinations", flush=True)

    except Exception as e:
        print(f"⚠️  Could not load historical baseline (first run?): {e}", flush=True)
        historical_baseline = {}


# ============================================================
# CONVICTION SCORE CALCULATOR
# ============================================================
def compute_conviction(reg_entry, oi_growth_from_open, hist_key):
    """
    Combines 5 signals into a single conviction score (0-150).
    Returns (score: int, label: str)
    """
    score = 0

    # --- Signal 1: OI growth from open (max 40 pts) ---
    # Fast OI growth = someone building a position in a hurry
    if oi_growth_from_open > 0:
        oi_pts = min(oi_growth_from_open * 0.6, 40)  # 67% growth = full 40pts
        score += oi_pts

    # --- Signal 2: Alert count for this exact strike today (max 30 pts) ---
    # Repeated visits to the same strike = deliberate, not random
    alert_count = reg_entry["alert_count"]
    if alert_count >= 10:
        score += 30
    elif alert_count >= 6:
        score += 20
    elif alert_count >= 3:
        score += 10
    elif alert_count >= 2:
        score += 5

    # --- Signal 3: Time active (max 20 pts) ---
    # Longer duration of activity = more deliberate accumulation
    time_active_minutes = reg_entry["time_active_minutes"]
    if time_active_minutes >= 90:
        score += 20
    elif time_active_minutes >= 45:
        score += 15
    elif time_active_minutes >= 20:
        score += 8
    elif time_active_minutes >= 5:
        score += 3

    # --- Signal 4: Value acceleration (max 20 pts) ---
    # Are alert gaps shrinking? Someone getting more aggressive = event approaching
    buckets = reg_entry["alert_timestamps"]
    if len(buckets) >= 3:
        gaps = [buckets[i] - buckets[i-1] for i in range(1, len(buckets))]
        # Compare last gap to first gap
        first_gap = gaps[0]
        last_gap  = gaps[-1]
        if first_gap > 0 and last_gap < first_gap * 0.5:
            # Last gap is less than half the first gap = clear acceleration
            score += 20
        elif first_gap > 0 and last_gap < first_gap * 0.75:
            score += 10

    # --- Signal 5: Historical anomaly — today vs past days (max 40 pts) ---
    # Most powerful signal: this strike behaving unusually vs its own history
    hist = historical_baseline.get(hist_key)
    if hist and hist["trading_days"] >= 2:
        avg_daily_alerts = hist["avg_alerts_per_day"]
        if avg_daily_alerts > 0:
            anomaly_ratio = alert_count / avg_daily_alerts
            if anomaly_ratio >= 5:
                score += 40   # firing 5x its normal rate
            elif anomaly_ratio >= 3:
                score += 25
            elif anomaly_ratio >= 2:
                score += 12
        avg_daily_value = hist["avg_value_per_day"]
        if avg_daily_value > 0:
            value_ratio = reg_entry["cumulative_value"] / avg_daily_value
            if value_ratio >= 4:
                score += 10   # bonus: value also anomalous

    score = int(min(score, 150))

    if score >= CONVICTION_HIGH:
        label = "HIGH_CONVICTION"
    elif score >= CONVICTION_SUSPICIOUS:
        label = "SUSPICIOUS"
    elif score >= CONVICTION_WATCHING:
        label = "WATCHING"
    else:
        label = "NOISE"

    return score, label


# ============================================================
# RABBITMQ WORKER
# ============================================================
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
                # Log to DuckDB — now with new columns
                db_con.execute("""
                    INSERT INTO march_30_moves 
                    (timestamp, ticker, strike, option_type, value, ltp, oi, category,
                     ltp_change, oi_change, volume, alert_count, time_active_minutes,
                     oi_growth_from_open, conviction, conviction_label)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                """, [
                    alert_data['timestamp'],
                    alert_data['ticker'],
                    str(alert_data['strike']),
                    alert_data['option_type'],
                    alert_data['value'],
                    alert_data['ltp'],
                    int(alert_data['oi']),
                    alert_data['category'],
                    alert_data['ltp_change'],
                    alert_data['oi_change'],
                    alert_data['volume'],
                    alert_data['alert_count'],
                    alert_data['time_active_minutes'],
                    alert_data['oi_growth_from_open'],
                    alert_data['conviction'],
                    alert_data['conviction_label'],
                ])
                # Publish to RabbitMQ
                if self.channel and not self.channel.is_closed:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.queue_name,
                        body=json.dumps(alert_data),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
            except Exception as e:
                print(f"WORKER ERRO: {e}")
                self.connect()
            finally:
                alert_queue.task_done()


mq_worker = RabbitMQWorker()


# ============================================================
# UPSTOX AUTH
# ============================================================
def get_market_data_feed_authorize_v3():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    r = requests.get(url, headers=headers)
    return r.json()


# ============================================================
# INSTRUMENT MAP BUILDER
# ============================================================
def create_optimized_lookup(active_keys):
    if not os.path.exists("companies_only.csv"):
        print("❌ companies_only.csv missing!")
        return {}
    master_df = pd.read_csv("companies_only.csv")
    lookup = {}
    active_set = set(active_keys)
    for _, row in master_df.iterrows():
        ce_key = str(row['ce_instrument_key'])
        pe_key = str(row['pe_instrument_key'])
        if ce_key in active_set:
            lookup[ce_key] = {"name": row['name'], "strike": row['strike_price'], "type": "CE"}
        if pe_key in active_set:
            lookup[pe_key] = {"name": row['name'], "strike": row['strike_price'], "type": "PE"}
    return lookup


# ============================================================
# QUEUE WORKER — parse protobuf ticks, build trade_history
# ============================================================
async def queue_worker():
    while True:
        message = await data_queue.get()
        feed_response = pb.FeedResponse()
        try:
            feed_response.ParseFromString(message)
            now = time.time()

            for key, feed in feed_response.feeds.items():
                if not hasattr(feed, 'firstLevelWithGreeks') or not feed.HasField('firstLevelWithGreeks'):
                    continue

                flwg  = feed.firstLevelWithGreeks
                ltpc  = flwg.ltpc
                vtt   = float(flwg.vtt)
                ltt   = int(ltpc.ltt)
                price = float(ltpc.ltp)
                current_oi = float(flwg.oi) if hasattr(flwg, 'oi') else last_trade_info[key]["oi"]

                # --- OI BASELINE: lock after BASELINE_LOCK_TIME ---
                if key not in oi_baseline and current_oi > 0:
                    current_hhmm = datetime.now().strftime("%H:%M")
                    if current_hhmm >= BASELINE_LOCK_TIME:
                        oi_baseline[key] = current_oi

                # --- Deduplicate ticks: only process genuinely new trades ---
                if ltt > last_trade_info[key]["ltt"] or vtt > last_trade_info[key]["vtt"]:
                    new_qty = (vtt - last_trade_info[key]["vtt"]
                               if last_trade_info[key]["vtt"] > 0
                               else float(ltpc.ltq))
                    if new_qty > 0:
                        trade_history[key].append((ltt / 1000, price * new_qty, price, current_oi, new_qty))
                    last_trade_info[key].update({"ltt": ltt, "vtt": vtt, "oi": current_oi})


                    #       notes #
                    #   the whole purpose of the last_trade_info dictionary is to prevent duplication 

        except:
            pass
        finally:
            data_queue.task_done()


# ============================================================
# ENERGY MONITOR — detect accumulation windows, fire alerts
# ============================================================
async def energy_monitor():
    print("⚡ Scanner Active...", flush=True)
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()

        for key in list(trade_history.keys()):
            # Trim window
            trade_history[key] = [t for t in trade_history[key] if now - t[0] <= WINDOW_TIME]
            if not trade_history[key]:
                continue

            current_trades = trade_history[key]
            val          = sum(t[1] for t in current_trades)
            total_volume = sum(t[4] for t in current_trades)

            if val < MIN_VAL_THRESHOLD:
                continue

            # --- PRICE DIRECTION ---
            first_price  = current_trades[0][2]
            last_price   = current_trades[-1][2]
            price_change = last_price - first_price
            price_pct    = (price_change / first_price) if first_price > 0 else 0
            PRICE_DEAD_ZONE = 0.003

            # --- OI DIRECTION ---
            first_oi  = current_trades[0][3]
            last_oi   = current_trades[-1][3]
            oi_change = last_oi - first_oi
            OI_DEAD_ZONE = 0.005
            oi_pct    = (oi_change / first_oi) if first_oi > 0 else 0
            oi_rising  = oi_pct >  OI_DEAD_ZONE
            oi_falling = oi_pct < -OI_DEAD_ZONE

            avg_trade_size = val / len(current_trades)

            # --- CATEGORY CLASSIFICATION (unchanged logic) ---
            if abs(price_pct) <= PRICE_DEAD_ZONE:
                category = "ACCUMULATION" if oi_rising else ("EXITING" if oi_falling else "ACCUMULATION")
            else:
                if price_change > 0 and oi_rising:
                    category = "LONG_BUILDUP"
                elif price_change < 0 and oi_rising:
                    category = "SHORT_BUILDUP"
                elif price_change > 0 and oi_falling:
                    category = "SHORT_COVERING"
                elif price_change < 0 and oi_falling:
                    category = "LONG_UNWINDING"
                else:
                    category = "LONG_BUILDUP" if price_change > 0 else "SHORT_BUILDUP"

            # --------------------------------------------------------
            # STRIKE REGISTRY — update persistent memory for this key
            # --------------------------------------------------------
            info     = INSTRUMENT_MAP.get(key, {"name": key, "strike": "", "type": ""})
            reg_key  = f"{info['name']}-{info['strike']}-{info['type']}"
            hist_key = reg_key   # same key used for historical lookup

            if reg_key not in strike_registry:
                strike_registry[reg_key] = {
                    "first_seen":        now,
                    "alert_count":       0,
                    "cumulative_value":  0.0,
                    "alert_timestamps":  [],   # list of epoch times when each alert fired
                }

            reg = strike_registry[reg_key]
            reg["alert_count"]      += 1
            reg["cumulative_value"] += val
            reg["alert_timestamps"].append(now)
            reg["time_active_minutes"] = (now - reg["first_seen"]) / 60

            # --------------------------------------------------------
            # OI GROWTH FROM OPEN
            # --------------------------------------------------------
            baseline_oi = oi_baseline.get(key, 0)
            if baseline_oi > 0:
                oi_growth_from_open = ((last_oi - baseline_oi) / baseline_oi) * 100
            else:
                oi_growth_from_open = 0.0

            # --------------------------------------------------------
            # CONVICTION SCORE
            # --------------------------------------------------------
            conviction, conviction_label = compute_conviction(reg, oi_growth_from_open, hist_key)

            # --------------------------------------------------------
            # BUILD AND QUEUE ALERT
            # --------------------------------------------------------
            alert_data = {
                "timestamp":           datetime.fromtimestamp(current_trades[-1][0]).strftime('%d/%m/%Y %H:%M:%S'),
                "ticker":              info['name'],
                "strike":              str(info['strike']),
                "option_type":         info['type'],
                "value":               round(val, 2),
                "ltp":                 round(last_price, 2),
                "category":            category,
                "oi":                  int(last_oi),
                "ltp_change":          round(price_change, 2),
                "oi_change":           round(oi_change, 0),
                "volume":              round(total_volume, 0),
                "avg_trade_size":      round(avg_trade_size, 0),
                # --- NEW FIELDS ---
                "alert_count":         reg["alert_count"],
                "time_active_minutes": round(reg["time_active_minutes"], 1),
                "oi_growth_from_open": round(oi_growth_from_open, 1),
                "cumulative_value":    round(reg["cumulative_value"], 2),
                "conviction":          conviction,
                "conviction_label":    conviction_label,
            }

            alert_queue.put_nowait(alert_data)
            trade_history[key].clear()

            # Print HIGH_CONVICTION alerts to terminal immediately
            if conviction_label == "HIGH_CONVICTION":
                print(
                    f"🔥 HIGH CONVICTION | {info['name']} {info['strike']} {info['type']} "
                    f"| Score: {conviction} | OI+{oi_growth_from_open:.0f}% | "
                    f"Alerts today: {reg['alert_count']} | "
                    f"Active: {reg['time_active_minutes']:.0f}min | "
                    f"₹{reg['cumulative_value']/100000:.1f}L total"
                    f"{datetime.now().strftime('%H:%M')}",
                    flush=True
                )


# ============================================================
# MAIN WEBSOCKET LOOP
# ============================================================
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
                sub_msg = {
                    "guid": "surge",
                    "method": "sub",
                    "data": {"mode": "option_greeks", "instrumentKeys": instrument_list}
                }
                await ws.send(json.dumps(sub_msg).encode("utf-8"))
                while True:
                    data_queue.put_nowait(await ws.recv())
        except:
            await asyncio.sleep(5)


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    print("Starting on", time.strftime('%b %d %H:%M:%S'))

    if not os.path.exists("atm_option_table.csv"):
        print("❌ Error: atm_option_table.csv not found! Run getting_strikes.py first.")
    else:
        # Load historical baseline BEFORE starting the feed
        load_historical_baseline()

        df = pd.read_csv("atm_option_table.csv")
        print(df.columns.tolist())

        instrument_cols = df.columns[3:]
        keys = [
            str(x) for x in set(df[instrument_cols].values.flatten().tolist())
            if str(x) != 'nan' and str(x) != 'None'
        ]
        print(f"Total unique instruments: {len(keys)}")

        if keys:
            print(f"✅ Subscribing to {len(keys)} instruments from {len(df)} stocks...")
            INSTRUMENT_MAP = create_optimized_lookup(keys)
            print("\n\n\n\n")
            print(INSTRUMENT_MAP)
            print(type(INSTRUMENT_MAP))
            print(len(INSTRUMENT_MAP))
            print("\n\n\n\n")
            asyncio.run(fetch_market_data(keys))
        else:
            print("❌ No valid instrument keys found in atm_option_table.csv")