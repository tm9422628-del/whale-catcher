import pandas as pd  
import requests


url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"



df = pd.read_json(url)
df_filtered = df[df['segment'] == "NSE_FO"]

df_filtered = df_filtered[df_filtered["instrument_type"].isin(["PE", "CE"])]
# print(df.columns)
columns_to_keep = ["name","expiry","instrument_type","asset_symbol","instrument_key","strike_price","lot_size","exchange_token","tick_size","trading_symbol","underlying_key"]
df_filtered = df_filtered[columns_to_keep]
# print(df_filtered.columns)

df_filtered["expiry"] = pd.to_datetime(df_filtered["expiry"], unit="ms")



#HERE WE ARE GIVING DECEMBER 30TH AS THE REQUIRED EXPIRY DATE 
df_filtered = df_filtered[df_filtered['expiry'] == "2026-02-24 18:29:59"]


df_filtered['instrument_type'] = pd.Categorical(
    df_filtered['instrument_type'], 
    categories=['PE', 'CE'], 
    ordered=True
)


df_filtered['strike_price'] = pd.to_numeric(df_filtered['strike_price'], errors='coerce')


df_sorted = df_filtered.sort_values(
    by=['name', 'instrument_type', 'strike_price'],
    ascending=[True, True, True]
).reset_index(drop=True)






df = df_sorted



df_out = (
    df
    .pivot_table(
        index=[
            'name',
            'asset_symbol',
            'expiry',
            'strike_price',
            'lot_size',
            'tick_size',
            'underlying_key'
        ],
        columns='instrument_type',
        values='instrument_key',
        aggfunc='first',
        observed=True
    )
    .reset_index()
)



df_out = df_out.rename(
    columns={
        'CE': 'ce_instrument_key',
        'PE': 'pe_instrument_key'
    }
)


df_out = df_out[
    [
        'name',
        'asset_symbol',
        'expiry',
        'ce_instrument_key',
        'strike_price',
        'pe_instrument_key',
        'lot_size',
        'tick_size',
        'underlying_key'
    ]
]
 


df_out.to_pickle("available_to_trade.pkl")
df_out.to_csv("available_to_trade.csv")






companies_df = df_out[~df_out["underlying_key"].str.contains("NSE_INDEX", na=False)]
companies_df = companies_df[~companies_df["name"].str.contains("RELIANCE", na=False)]


companies_df.to_csv("companies_only.csv", index=False)



companies = companies_df['name'].dropna().unique()
underlying_keys = companies_df['underlying_key'].dropna().unique()



print(len(companies))




with open("companies.txt","w") as f:
    for company in companies:
        f.write(f"{company}\n")


with open("underlying_keys.txt","w") as f:
    for underlying_key in underlying_keys:
        f.write(f"{underlying_key}\n")












