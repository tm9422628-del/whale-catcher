import asyncio
import aiohttp
import async_timeout
import pandas as pd
import numpy as np
import time
from dotenv import load_dotenv
import os

load_dotenv()   
TOKEN = os.getenv("token")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

API_URL = "https://api.upstox.com/v3/market-quote/ltp?instrument_key="
CSV_PATH = "companies_only.csv"

# ===============================
# RATE LIMIT CONFIG
# ===============================
MAX_CONCURRENT_REQUESTS = 10
REQUESTS_PER_SECOND = 40
RETRY_LIMIT = 6

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
rate_lock = asyncio.Lock()
last_request_ts = 0.0

async def rate_limit():
    global last_request_ts
    async with rate_lock:
        now = time.monotonic()
        wait = max(0, (1 / REQUESTS_PER_SECOND) - (now - last_request_ts))
        if wait > 0:
            await asyncio.sleep(wait)
        last_request_ts = time.monotonic()

async def fetch_ltp(session, instrument_key):
    url = API_URL + instrument_key
    for attempt in range(RETRY_LIMIT):
        async with semaphore:
            await rate_limit()
            try:
                async with async_timeout.timeout(5):
                    async with session.get(url, headers=HEADERS) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            info = next(iter(data["data"].values()))
                            return instrument_key, info["last_price"]
                        await asyncio.sleep(0.4)
            except:
                await asyncio.sleep(0.3)
    return instrument_key, None

async def fetch_all_ltps(keys):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ltp(session, k) for k in keys]
        results = await asyncio.gather(*tasks)
    return dict(results)

# ===============================
# DIRECTIONAL STRIKE LOGIC
# ===============================
def build_directional_table(df, ltp_map):
    rows = []
    # We want ATM (0), Minus offsets (-3 to -6), and Plus offsets (3 to 6)
    for underlying, g in df.groupby("underlying_key"):
        spot = ltp_map.get(underlying)
        if not spot: continue

        g = g.sort_values("strike_price")
        strikes = g["strike_price"].values
        atm_idx = np.abs(strikes - spot).argmin()

        entry = {
            "name": g.iloc[0]["name"],
            "underlying_key": underlying,
            "spot_price": spot
        }

        def get_key(idx, opt_type):
            if 0 <= idx < len(strikes):
                row = g[g["strike_price"] == strikes[idx]].iloc[0]
                return row[f"{opt_type.lower()}_instrument_key"]
            return None

        # 1. ATM - Keep both because trend can go either way from here
        entry["atm_ce"] = get_key(atm_idx, "CE")
        entry["atm_pe"] = get_key(atm_idx, "PE")

        # 2. MINUS OFFSETS (-3, -4, -5, -6) -> ONLY PE (Downside protection/bets)
        for offset in [-4, -5, -6]:
            label = f"minus_{abs(offset)}_pe"
            entry[label] = get_key(atm_idx + offset, "PE")

        # 3. PLUS OFFSETS (+3, +4, +5, +6) -> ONLY CE (Upside breakouts)
        for offset in [4, 5, 6]:
            label = f"plus_{offset}_ce"
            entry[label] = get_key(atm_idx + offset, "CE")

        rows.append(entry)

    return pd.DataFrame(rows)

async def main():
    if not os.path.exists(CSV_PATH):
        print(f"File {CSV_PATH} not found.")
        return
        
    df = pd.read_csv(CSV_PATH)
    underlying_keys = df["underlying_key"].unique().tolist()

    print(f"📡 Fetching spot prices for {len(underlying_keys)} stocks...")
    ltp_map = await fetch_all_ltps(underlying_keys)

    print("🛠️ Filtering for Directional Whale Strikes...")
    final_df = build_directional_table(df, ltp_map)

    final_df.to_csv("atm_option_table.csv", index=False)
    print(f"✅ Saved {len(final_df)} stocks to atm_option_table.csv (Cleaned Mode)")

if __name__ == "__main__":
    asyncio.run(main())