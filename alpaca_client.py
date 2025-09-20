from __future__ import annotations
import os, datetime as dt, pytz
from typing import List, Dict, Tuple, Optional
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetAssetsRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, AssetClass, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

APCA_KEY = os.environ.get("ALPACA_API_KEY_V3", "")
APCA_SEC = os.environ.get("ALPACA_SECRET_KEY_V3", "")
APCA_BASE_URL = os.getenv("APCA_BASE_URL", "https://paper-api.alpaca.markets")

def _trading_client():
    if not APCA_KEY or not APCA_SEC:
        raise RuntimeError("Missing Alpaca API keys.")
    return TradingClient(APCA_KEY, APCA_SEC, paper=APCA_BASE_URL.endswith("paper-api.alpaca.markets"))

def _data_client():
    if not APCA_KEY or not APCA_SEC:
        raise RuntimeError("Missing Alpaca API keys.")
    return StockHistoricalDataClient(APCA_KEY, APCA_SEC)

def get_account():
    return _trading_client().get_account()

def get_clock():
    return _trading_client().get_clock()

def is_market_open_now(buffer_min: int = 0) -> Tuple[bool, Dict]:
    clk = get_clock()
    now = clk.timestamp.replace(tzinfo=pytz.UTC)
    open_ = clk.is_open
    next_open = clk.next_open.replace(tzinfo=pytz.UTC) if clk.next_open else None
    next_close = clk.next_close.replace(tzinfo=pytz.UTC) if clk.next_close else None
    if not open_:
        return False, {"now": now, "open": open_, "next_open": next_open, "next_close": next_close}
    if buffer_min and next_close:
        if (next_close - now).total_seconds() <= buffer_min * 60:
            return False, {"now": now, "open": open_, "next_open": next_open, "next_close": next_close, "reason": "near_close"}
    return True, {"now": now, "open": open_, "next_open": next_open, "next_close": next_close}

def get_positions() -> Dict[str, Dict]:
    client = _trading_client()
    positions = {}
    for p in client.get_all_positions():
        positions[p.symbol] = {
            "qty": float(p.qty),
            "market_value": float(p.market_value),
            "avg_entry_price": float(p.avg_entry_price),
            "unrealized_pl": float(getattr(p, "unrealized_pl", 0.0))
        }
    return positions

def cancel_all_orders():
    client = _trading_client()
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
    open_orders = client.get_orders(filter=req)
    for o in open_orders:
        try:
            client.cancel_order_by_id(o.id)
        except Exception as e:
            print("Cancel error:", e)

def get_bars(symbols: List[str], days: int = 250) -> pd.DataFrame:
    data = _data_client()
    end = dt.datetime.now(pytz.UTC)
    start = end - dt.timedelta(days=int(days*1.5))
    out = []
    batch = 100
    for i in range(0, len(symbols), batch):
        syms = symbols[i:i+batch]
        req = StockBarsRequest(symbol_or_symbols=syms, timeframe=TimeFrame.Day, start=start, end=end, limit=days)
        df = data.get_stock_bars(req).df
        if df is None or df.empty: 
            continue
        if "symbol" in df.index.names:
            out.append(df)
        else:
            df["symbol"] = syms[0] if len(syms)==1 else None
            df = df.set_index("symbol", append=True).swaplevel(0,1).sort_index()
            out.append(df)
    if not out:
        return pd.DataFrame()
    df = pd.concat(out).sort_index()
    return df

def get_intraday_last_prices(symbols, minutes: int = 20) -> dict:
    data = _data_client()
    if not symbols: 
        return {}
    end = dt.datetime.now(pytz.UTC)
    start = end - dt.timedelta(minutes=minutes)
    req = StockBarsRequest(symbol_or_symbols=list(symbols), timeframe=TimeFrame.Minute, start=start, end=end, limit=1)
    df = data.get_stock_bars(req).df
    out = {}
    if df is None or df.empty:
        return out
    if "symbol" in df.index.names:
        for sym in set(df.index.get_level_values("symbol")):
            s = df.xs(sym, level="symbol")
            out[sym] = float(s["close"].iloc[-1])
    else:
        out[list(symbols)[0]] = float(df["close"].iloc[-1])
    return out

def submit_notional_order(symbol: str, notional: float, side: str):
    client = _trading_client()
    req = MarketOrderRequest(symbol=symbol, notional=round(notional,2), side=OrderSide.BUY if side.lower()=="buy" else OrderSide.SELL, time_in_force=TimeInForce.DAY)
    return client.submit_order(req)

def submit_qty_order(symbol: str, qty: float, side: str):
    client = _trading_client()
    req = MarketOrderRequest(symbol=symbol, qty=qty, side=OrderSide.BUY if side.lower()=="buy" else OrderSide.SELL, time_in_force=TimeInForce.DAY)
    return client.submit_order(req)

def list_fractionable(symbols: List[str]) -> Dict[str, bool]:
    client = _trading_client()
    res = {}
    assets = client.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY))
    sset = set(symbols)
    for a in assets:
        if a.symbol in sset:
            res[a.symbol] = bool(getattr(a, "fractionable", False))
    return res
