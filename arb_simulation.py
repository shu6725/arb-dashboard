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

### 1) BoardDataLogger (Compressed CSV)
class BoardDataLogger:
    def __init__(self, filename: str):
        self.filename = filename

    def write_row(self, exchange: str, pair: str, bid: float, ask: float, timestamp: float):
        row = [exchange, pair, bid, ask, timestamp]
        # Open in binary mode with lzma and wrap with a text stream for csv.writer
        with lzma.open(self.filename, 'ab') as compressed_file:
            with io.TextIOWrapper(compressed_file, encoding='utf-8', newline='') as text_file:
                writer = csv.writer(text_file)
                writer.writerow(row)

### 2) OrderHistoryDB (SQLite)
class OrderHistoryDB:
    def __init__(self, db_path: str = "arb_orders.db"):
        self.conn = sqlite3.connect(db_path)
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

### 3) Global order_book dictionary & profit threshold
order_book: Dict[Tuple[str, str], Dict[str, float]] = {}
# Set threshold to 10 basis points (0.1% as a fraction)
profit_threshold = 0.001  

def check_arbitrage(local_pair: str, ohdb: OrderHistoryDB):
    """
    Compare all exchange's bid/ask for the given local_pair.
    If the relative difference (bidA - askB) / askB exceeds 10bps (0.1%),
    record a trade in the DB.
    """
    relevant = []
    for (ex, p), info in order_book.items():
        if p == local_pair and "bid" in info and "ask" in info:
            relevant.append((ex, info["bid"], info["ask"]))

    for i in range(len(relevant)):
        for j in range(len(relevant)):
            if i == j:
                continue
            exA, bidA, askA = relevant[i]
            exB, bidB, askB = relevant[j]
            # Calculate the relative difference as a fraction of ask price
            diff = (bidA - askB) / askB
            if diff > profit_threshold:
                profit_percent = diff * 100  # Convert to percentage
                print(f"[ARBITRAGE] {local_pair}: SELL={exA}@{bidA}, BUY={exB}@{askB}, profit={profit_percent:.2f}%", flush=True)
                ohdb.insert_trade(local_pair, exA, exB, bidA, askB, profit_percent)

### 4) fetch_orderbook_ccxt
async def fetch_orderbook_ccxt(
    exchange_name: str,
    local_pair: str,
    actual_symbol: str,
    ccxt_exchange,
    data_logger: BoardDataLogger,
    ohdb: OrderHistoryDB
):
    """
    local_pair: e.g. "BTC/USD"
    actual_symbol: e.g. "BTC/USD" or "XBT/USD" or "BTC_JPY" etc., depending on the exchange
    """
    while True:
        try:
            ob = await ccxt_exchange.fetch_order_book(actual_symbol)
            best_bid = ob['bids'][0][0] if ob['bids'] else None
            best_ask = ob['asks'][0][0] if ob['asks'] else None
            if best_bid is not None and best_ask is not None:
                ts = time.time()
                order_book[(exchange_name, local_pair)] = {"bid": best_bid, "ask": best_ask, "ts": ts}
                data_logger.write_row(exchange_name, local_pair, best_bid, best_ask, ts)
                check_arbitrage(local_pair, ohdb)
            else:
                print(f"[{exchange_name}] No orderbook data: {actual_symbol}", flush=True)
        except Exception as e:
            print(f"[{exchange_name}] fetch error: {e}", flush=True)
        await asyncio.sleep(1)

### New: Continuously display the global order_book dictionary on terminal
async def display_orderbook():
    while True:
        print(f"order_book: {order_book}", flush=True)
        await asyncio.sleep(2)

### 5) Main logic
async def main():
    data_logger = BoardDataLogger("board_data.csv.xz")
    ohdb = OrderHistoryDB("arb_orders.db")

    # Setup ccxt exchange objects (async)
    coinbase = ccxt.coinbase()
    kraken = ccxt.kraken()
    bitflyer = ccxt.bitflyer()
    coincheck = ccxt.coincheck()

    # Load markets for each exchange
    await coinbase.load_markets()
    await kraken.load_markets()
    await bitflyer.load_markets()
    await coincheck.load_markets()

    # Mapping of local pairs to actual symbol names per exchange
    # For example, "BTC/USD" on coinbase, "BTC/USD" on kraken, "BTC_JPY" on bitflyer, etc.
    exchange_pairs = [
        # (exchange_name, local_pair, actual_symbol, exchange_obj)
        ("coinbase",  "BTC/USD", "BTC/USD", coinbase),
        ("coinbase",  "ETH/USD", "ETH/USD", coinbase),
        ("kraken",    "BTC/USD", "BTC/USD", kraken),   # Note: Kraken may use XBT for BTC
        ("kraken",    "ETH/USD", "ETH/USD", kraken),
        ("bitflyer",  "BTC/JPY", "BTC_JPY", bitflyer),
        ("bitflyer",  "ETH/JPY", "ETH_JPY", bitflyer),
        ("coincheck", "BTC/JPY", "btc_jpy", coincheck),
        ("coincheck", "ETH/JPY", "eth_jpy", coincheck),
    ]

    tasks = []
    # Create tasks for fetching orderbooks from each exchange
    for ex_name, local_pair, actual_symbol, ex_obj in exchange_pairs:
        tasks.append(asyncio.create_task(
            fetch_orderbook_ccxt(ex_name, local_pair, actual_symbol, ex_obj, data_logger, ohdb)
        ))
    # Create task to display the global order_book continuously
    tasks.append(asyncio.create_task(display_orderbook()))

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

    # Close exchange connections (this part may not be reached if tasks run indefinitely)
    await coinbase.close()
    await kraken.close()
    await bitflyer.close()
    await coincheck.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutdown requested.", flush=True)