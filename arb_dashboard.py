#!/usr/bin/env python
# coding: utf-8

import ccxt.async_support as ccxt
import asyncio
import time
import lzma
import io
import csv
import sqlite3
from datetime import datetime
from typing import Dict, Tuple

import threading
import pandas as pd
from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
import aiohttp
from dash_table.Format import Format, Scheme

# Global start time (only show trades after program start)
START_TIME = datetime.utcnow().isoformat()
# Global variable to store the latest USD/JPY rate (for converting USD-denominated prices to JPY)
usd_jpy_rate = None

#########################################
# 1) BoardDataLogger (Compressed CSV)   #
#########################################
class BoardDataLogger:
    def __init__(self, filename: str):
        self.filename = filename

    def write_row(self, exchange: str, pair: str, bid: float, ask: float, timestamp: float):
        row = [exchange, pair, bid, ask, timestamp]
        with lzma.open(self.filename, 'ab') as compressed_file:
            with io.TextIOWrapper(compressed_file, encoding='utf-8', newline='') as text_file:
                writer = csv.writer(text_file)
                writer.writerow(row)

#########################################
# 2) OrderHistoryDB (SQLite)            #
#########################################
class OrderHistoryDB:
    def __init__(self, db_path: str = "arb_orders.db"):
        # Allow access from other threads (e.g. Dash callbacks)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.init_table()

    def init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS arb_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            pair TEXT,
            sell_exchange TEXT,
            buy_exchange TEXT,
            sell_price REAL,
            buy_price REAL,
            profit REAL
        )
        """)
        self.conn.commit()

    def insert_trade(self, pair, sell_ex, buy_ex, sell_px, buy_px, profit):
        now_str = datetime.utcnow().isoformat()
        cur = self.conn.cursor()
        cur.execute("""
        INSERT INTO arb_trades
            (timestamp, pair, sell_exchange, buy_exchange, sell_price, buy_price, profit)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now_str, pair, sell_ex, buy_ex, sell_px, buy_px, profit))
        self.conn.commit()

#########################################
# 3) Global Order Book & Threshold      #
#########################################
order_book: Dict[Tuple[str, str], Dict[str, float]] = {}
# Arbitrage threshold as a fraction (10bps = 0.001)
profit_threshold = 0.001  

def check_arbitrage(local_pair: str, ohdb: OrderHistoryDB):
    """
    Compare bid/ask across exchanges for the given pair.
    Before comparing, if an entry was originally USD-denominated,
    convert its bid/ask to JPY using the global usd_jpy_rate.
    If any pair of exchanges has a relative difference (bidA - askB)/askB > 10bps,
    record the trade.
    """
    relevant = []
    for (ex, p), info in order_book.items():
        if p == local_pair and "bid" in info and "ask" in info:
            bid = info["bid"]
            ask = info["ask"]
            # If this entry is from a USD market, convert its values using usd_jpy_rate.
            if info.get("usd", False) and usd_jpy_rate is not None:
                bid = bid * usd_jpy_rate
                ask = ask * usd_jpy_rate
            relevant.append((ex, bid, ask))
    for i in range(len(relevant)):
        for j in range(len(relevant)):
            if i == j:
                continue
            exA, bidA, askA = relevant[i]
            exB, bidB, askB = relevant[j]
            diff = (bidA - askB) / askB
            if diff > profit_threshold:
                profit_percent = diff * 100
                print(f"[ARBITRAGE] {local_pair}: SELL={exA}@{bidA}, BUY={exB}@{askB}, profit={profit_percent:.2f}%", flush=True)
                ohdb.insert_trade(local_pair, exA, exB, bidA, askB, profit_percent)

#########################################
# 4) fetch_orderbook_ccxt                #
#########################################
async def fetch_orderbook_ccxt(
    exchange_name: str,
    local_pair: str,
    actual_symbol: str,
    ccxt_exchange,
    data_logger: BoardDataLogger,
    ohdb: OrderHistoryDB
):
    """
    Fetch order book for the given symbol and update the global order_book.
    local_pair: e.g. "BTC/USD", "ETH/USD", "XRP/USD", "XLM/USD" (for USD markets)
                or "BTC/JPY", "ETH/JPY", "XRP/JPY", "XLM/JPY" (native JPY markets)
    actual_symbol: as required by ccxt (e.g. "BTC/USD", "XRP/USD", etc.)
    """
    while True:
        try:
            ob = await ccxt_exchange.fetch_order_book(actual_symbol)
            best_bid = ob['bids'][0][0] if ob['bids'] else None
            best_ask = ob['asks'][0][0] if ob['asks'] else None
            if best_bid is not None and best_ask is not None:
                ts = time.time()
                coin = local_pair.split("/")[0]
                # Normalize USD pairs to JPY format; mark for conversion
                if local_pair.endswith("/USD"):
                    normalized_pair = f"{coin}/JPY"
                    needs_conversion = True
                else:
                    normalized_pair = local_pair
                    needs_conversion = False
                order_book[(exchange_name, normalized_pair)] = {
                    "bid": best_bid,
                    "ask": best_ask,
                    "ts": ts,
                    "usd": needs_conversion
                }
                data_logger.write_row(exchange_name, normalized_pair, best_bid, best_ask, ts)
                check_arbitrage(normalized_pair, ohdb)
            else:
                print(f"[{exchange_name}] No orderbook data: {actual_symbol}", flush=True)
        except Exception as e:
            print(f"[{exchange_name}] fetch error: {e}", flush=True)
        await asyncio.sleep(1)

#########################################
# 5) Fetch USDJPY Rate from coin.z.com   #
#########################################
async def fetch_usd_jpy_rate():
    global usd_jpy_rate
    url = "https://forex-api.coin.z.com/public/v1/ticker"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url) as resp:
                    data = await resp.json()
                    # Look for the USD_JPY ticker in the response data array
                    for item in data.get("data", []):
                        if item.get("symbol") == "USD_JPY":
                            bid = float(item.get("bid", "0"))
                            ask = float(item.get("ask", "0"))
                            usd_jpy_rate = (bid + ask) / 2
                            # print(f"Updated USDJPY rate: {usd_jpy_rate}", flush=True)
                            break
            except Exception as e:
                print(f"Error fetching USDJPY rate: {e}", flush=True)
            await asyncio.sleep(5)

#########################################
# 6) Unified Order Book DataFrame         #
#########################################
def get_unified_order_book_df():
    """
    Build a unified DataFrame with all order-book rows converted to JPY.
    For USD-denominated rows (flag usd=True) multiply prices by usd_jpy_rate.
    Also add a 'Coin' column (BTC, ETH, XRP, or XLM).
    Then, for each coin group, compute best bid/ask and append a Best row.
    """
    global usd_jpy_rate
    data = []
    for (ex, p), info in order_book.items():
        coin = p.split("/")[0]
        # Determine conversion if needed from stored flag
        if info.get("usd", False):
            if usd_jpy_rate is not None:
                bid = info.get("bid") * usd_jpy_rate
                ask = info.get("ask") * usd_jpy_rate
            else:
                bid = info.get("bid")
                ask = info.get("ask")
        else:
            bid = info.get("bid")
            ask = info.get("ask")
        data.append({
            "Coin": coin,
            "Exchange": ex,
            "Bid": bid,
            "Ask": ask,
            "Diff": None,         # To be computed for Best row
            "RelDiffPct": None    # Hidden value used for styling
        })
    if not data:
        return pd.DataFrame(data)
    df = pd.DataFrame(data)
    df_list = []
    # Group by coin and compute best bid, best ask, diff etc.
    for coin, group in df.groupby("Coin"):
        best_bid = group["Bid"].max()
        best_ask = group["Ask"].min()
        diff = best_bid - best_ask
        rel_diff_pct = (diff / best_ask) * 100 if best_ask > 0 else 0
        group = group.copy()
        group["is_best_bid"] = group["Bid"] == best_bid
        group["is_best_ask"] = group["Ask"] == best_ask
        best_row = {
            "Coin": coin,
            "Exchange": "Best",
            "Bid": best_bid,
            "Ask": best_ask,
            "Diff": float(f"{diff:.5g}"),
            "RelDiffPct": rel_diff_pct,
            "is_best_bid": False,
            "is_best_ask": False
        }
        best_df = pd.DataFrame([best_row])
        group = pd.concat([group, best_df], ignore_index=True)
        df_list.append(group)
    df_final = pd.concat(df_list, ignore_index=True)
    return df_final

#########################################
# 7) Main Async Logic                   #
#########################################
async def main_async():
    data_logger = BoardDataLogger("board_data.csv.xz")
    ohdb = OrderHistoryDB("arb_orders.db")

    coinbase = ccxt.coinbase()
    kraken = ccxt.kraken()
    bitflyer = ccxt.bitflyer()
    coincheck = ccxt.coincheck()

    # Load markets for each exchange
    await coinbase.load_markets()
    await kraken.load_markets()
    await bitflyer.load_markets()
    await coincheck.load_markets()

    # Define exchange pairs.
    # For USD markets, the pair (e.g. "XRP/USD") is normalized to "XRP/JPY" and marked for conversion.
    exchange_pairs = [
        ("coinbase", "BTC/USD", "BTC/USD", coinbase),
        ("coinbase", "ETH/USD", "ETH/USD", coinbase),
        ("coinbase", "XRP/USD", "XRP/USD", coinbase),
        ("coinbase", "XLM/USD", "XLM/USD", coinbase),
        ("kraken",   "BTC/USD", "BTC/USD", kraken),
        ("kraken",   "ETH/USD", "ETH/USD", kraken),
        ("kraken",   "XRP/USD", "XRP/USD", kraken),
        ("kraken",   "XLM/USD", "XLM/USD", kraken),
        ("bitflyer", "BTC/JPY", "BTC_JPY", bitflyer),
        ("bitflyer", "ETH/JPY", "ETH_JPY", bitflyer),
        ("bitflyer", "XRP/JPY", "XRP_JPY", bitflyer),
        ("bitflyer", "XLM/JPY", "XLM_JPY", bitflyer),
        ("coincheck", "BTC/JPY", "btc_jpy", coincheck),
        ("coincheck", "ETH/JPY", "eth_jpy", coincheck),
        ("coincheck", "XRP/JPY", "xrp_jpy", coincheck),
        ("coincheck", "XLM/JPY", "xlm_jpy", coincheck),
    ]

    tasks = []
    for ex_name, local_pair, actual_symbol, ex_obj in exchange_pairs:
        tasks.append(asyncio.create_task(
            fetch_orderbook_ccxt(ex_name, local_pair, actual_symbol, ex_obj, data_logger, ohdb)
        ))
    # Also start the USDJPY rate fetcher
    tasks.append(asyncio.create_task(fetch_usd_jpy_rate()))
    await asyncio.gather(*tasks)

    await coinbase.close()
    await kraken.close()
    await bitflyer.close()
    await coincheck.close()

#########################################
# 8) Dashboard Helper for Arbitrage Trades#
#########################################
def get_arb_trades_df(pair_filter: str, start_time: str):
    conn = sqlite3.connect("arb_orders.db")
    query = "SELECT * FROM arb_trades WHERE timestamp >= ?"
    params = [start_time]
    if pair_filter and pair_filter != "All":
        query += " AND pair = ?"
        params.append(pair_filter)
    query += " ORDER BY id DESC LIMIT 100"
    try:
        df = pd.read_sql(query, conn, params=params)
    except Exception as e:
        print(f"DB read error: {e}", flush=True)
        df = pd.DataFrame()
    finally:
        conn.close()
    if not df.empty:
        df["profit_quote"] = df["sell_price"] - df["buy_price"]
    return df

#########################################
# 9) Dash Dashboard Implementation      #
#########################################
app = Dash(__name__)
app.layout = html.Div([
    # Header: Display the latest USDJPY rate at the top of the page
    html.Div(id="usd-jpy-rate", style={"font-size": "24px", "font-weight": "bold", "padding": "10px"}),
    html.H1("Arbitrage Dashboard"),
    # Order Book section: Two rows of two tables each
    html.Div([
        # First row: BTC and ETH order books
        html.Div([
            html.Div([
                html.H3("BTC/JPY Order Book"),
                dash_table.DataTable(
                    id="order-book-BTC_JPY",
                    columns=[
                        {"name": "Exchange", "id": "Exchange"},
                        {"name": "Bid", "id": "Bid", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                        {"name": "Ask", "id": "Ask", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                        {"name": "Diff", "id": "Diff", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)}
                    ],
                    data=[],
                    hidden_columns=["is_best_bid", "is_best_ask", "RelDiffPct"],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Exchange} = "Best" && {RelDiffPct} > 0.1',
                                'column_id': 'Diff'
                            },
                            'backgroundColor': 'yellow'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_bid} eq True',
                                'column_id': 'Bid'
                            },
                            'backgroundColor': 'lightgreen'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_ask} eq True',
                                'column_id': 'Ask'
                            },
                            'backgroundColor': 'tomato'
                        }
                    ]
                )
            ], style={'flex': 1, 'padding': '10px'}),
            html.Div([
                html.H3("ETH/JPY Order Book"),
                dash_table.DataTable(
                    id="order-book-ETH_JPY",
                    columns=[
                        {"name": "Exchange", "id": "Exchange"},
                        {"name": "Bid", "id": "Bid", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                        {"name": "Ask", "id": "Ask", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                        {"name": "Diff", "id": "Diff", "type": "numeric",
                         "format": Format(precision=0, scheme=Scheme.fixed, group=True)}
                    ],
                    data=[],
                    hidden_columns=["is_best_bid", "is_best_ask", "RelDiffPct"],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Exchange} = "Best" && {RelDiffPct} > 0.1',
                                'column_id': 'Diff'
                            },
                            'backgroundColor': 'yellow'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_bid} eq True',
                                'column_id': 'Bid'
                            },
                            'backgroundColor': 'lightgreen'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_ask} eq True',
                                'column_id': 'Ask'
                            },
                            'backgroundColor': 'tomato'
                        }
                    ]
                )
            ], style={'flex': 1, 'padding': '10px'})
        ], style={'display': 'flex', 'flexDirection': 'row'}),
        # Second row: XRP and XLM order books (2 decimal places)
        html.Div([
            html.Div([
                html.H3("XRP/JPY Order Book"),
                dash_table.DataTable(
                    id="order-book-XRP_JPY",
                    columns=[
                        {"name": "Exchange", "id": "Exchange"},
                        {"name": "Bid", "id": "Bid", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)},
                        {"name": "Ask", "id": "Ask", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)},
                        {"name": "Diff", "id": "Diff", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)}
                    ],
                    data=[],
                    hidden_columns=["is_best_bid", "is_best_ask", "RelDiffPct"],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Exchange} = "Best" && {RelDiffPct} > 0.1',
                                'column_id': 'Diff'
                            },
                            'backgroundColor': 'yellow'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_bid} eq True',
                                'column_id': 'Bid'
                            },
                            'backgroundColor': 'lightgreen'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_ask} eq True',
                                'column_id': 'Ask'
                            },
                            'backgroundColor': 'tomato'
                        }
                    ]
                )
            ], style={'flex': 1, 'padding': '10px'}),
            html.Div([
                html.H3("XLM/JPY Order Book"),
                dash_table.DataTable(
                    id="order-book-XLM_JPY",
                    columns=[
                        {"name": "Exchange", "id": "Exchange"},
                        {"name": "Bid", "id": "Bid", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)},
                        {"name": "Ask", "id": "Ask", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)},
                        {"name": "Diff", "id": "Diff", "type": "numeric",
                         "format": Format(precision=2, scheme=Scheme.fixed, group=True)}
                    ],
                    data=[],
                    hidden_columns=["is_best_bid", "is_best_ask", "RelDiffPct"],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Exchange} = "Best" && {RelDiffPct} > 0.1',
                                'column_id': 'Diff'
                            },
                            'backgroundColor': 'yellow'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_bid} eq True',
                                'column_id': 'Bid'
                            },
                            'backgroundColor': 'lightgreen'
                        },
                        {
                            'if': {
                                'filter_query': '{is_best_ask} eq True',
                                'column_id': 'Ask'
                            },
                            'backgroundColor': 'tomato'
                        }
                    ]
                )
            ], style={'flex': 1, 'padding': '10px'})
        ], style={'display': 'flex', 'flexDirection': 'row', 'margin-top': '20px'})
    ]),
    # Arbitrage Trades section with pair filter dropdown
    html.Div([
        html.H2("Arbitrage Trades"),
        dcc.Dropdown(
            id="pair-dropdown",
            options=[
                {"label": "All", "value": "All"},
                {"label": "BTC/JPY", "value": "BTC/JPY"},
                {"label": "ETH/JPY", "value": "ETH/JPY"},
                {"label": "XRP/JPY", "value": "XRP/JPY"},
                {"label": "XLM/JPY", "value": "XLM/JPY"}
            ],
            value="All",
            placeholder="Filter by pair"
        ),
        dash_table.DataTable(
            id="arb-trades-table",
            columns=[
                {"name": "ID", "id": "id"},
                {"name": "Timestamp", "id": "timestamp"},
                {"name": "Pair", "id": "pair"},
                {"name": "Sell Exchange", "id": "sell_exchange"},
                {"name": "Buy Exchange", "id": "buy_exchange"},
                {"name": "Sell Price", "id": "sell_price", "type": "numeric",
                 "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                {"name": "Buy Price", "id": "buy_price", "type": "numeric",
                 "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                {"name": "Profit (%)", "id": "profit", "type": "numeric",
                 "format": Format(precision=0, scheme=Scheme.fixed, group=True)},
                {"name": "Profit (Quote)", "id": "profit_quote", "type": "numeric",
                 "format": Format(precision=0, scheme=Scheme.fixed, group=True)}
            ],
            data=[]
        )
    ]),
    dcc.Interval(
        id="interval-component",
        interval=2000,  # update every 2 seconds
        n_intervals=0
    )
])

# Callback to update the USDJPY rate header
@app.callback(
    Output("usd-jpy-rate", "children"),
    Input("interval-component", "n_intervals")
)
def update_usd_jpy_rate_header(n):
    global usd_jpy_rate
    if usd_jpy_rate is not None:
        return f"USDJPY Rate: {usd_jpy_rate:.3f}"
    else:
        return "USDJPY Rate: Loading..."

# Callback to update the order book tables for BTC, ETH, XRP, and XLM (all in JPY)
@app.callback(
    [Output("order-book-BTC_JPY", "data"),
     Output("order-book-ETH_JPY", "data"),
     Output("order-book-XRP_JPY", "data"),
     Output("order-book-XLM_JPY", "data")],
    Input("interval-component", "n_intervals")
)
def update_order_books(n):
    df = get_unified_order_book_df()
    df_btc = df[df["Coin"] == "BTC"]
    df_eth = df[df["Coin"] == "ETH"]
    df_xrp = df[df["Coin"] == "XRP"]
    df_xlm = df[df["Coin"] == "XLM"]
    return (df_btc.to_dict("records"),
            df_eth.to_dict("records"),
            df_xrp.to_dict("records"),
            df_xlm.to_dict("records"))

# Callback to update arbitrage trades table based on interval and dropdown filter
@app.callback(
    Output("arb-trades-table", "data"),
    [Input("interval-component", "n_intervals"),
     Input("pair-dropdown", "value")]
)
def update_arb_trades(n, pair_value):
    df = get_arb_trades_df(pair_value, START_TIME)
    return df.to_dict("records")

def run_dash():
    app.run_server(debug=False, use_reloader=False)

#########################################
# Main Entry Point                      #
#########################################
if __name__ == "__main__":
    dash_thread = threading.Thread(target=run_dash, daemon=True)
    dash_thread.start()
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("Shutdown requested.", flush=True)