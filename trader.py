from __future__ import annotations
import os, json, pytz, datetime as dt, pathlib
import pandas as pd

from settings_store import get_settings, init_settings_table
from alpaca_client import (get_account, get_positions, get_bars, submit_notional_order, submit_qty_order, cancel_all_orders, is_market_open_now, list_fractionable, get_intraday_last_prices)
from strategy import compute_features, compute_breadth, spy_regime, risk_weights_for
from llm_policy import choose_portfolio
from memory import init_db, insert_episode, insert_picks, insert_order, build_memory_context, insert_log

DATA_DIR = pathlib.Path(__file__).parent / "data"
LOG_DIR = pathlib.Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def load_universe(mode: str = "sp500_etfs") -> list[str]:
    etfs = [s.strip() for s in open(DATA_DIR/"etfs_large.csv").read().strip().splitlines() if s.strip()]
    if mode == "etfs_only":
        return etfs
    sp500 = []
    try:
        import pandas as pd
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        sp500 = list(pd.read_csv(url)["Symbol"].dropna().astype(str).str.upper().unique())
    except Exception:
        sp500 = [s.strip() for s in open(DATA_DIR/"sp500_fallback.csv").read().strip().splitlines() if s.strip()]
    return sorted(set(sp500 + etfs))

def within_time_window_et(now_utc: dt.datetime, windows_csv: str) -> bool:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    tstr = ny.strftime("%H:%M")
    return tstr in set(w.strip() for w in windows_csv.split(","))

def within_market_hours_et(now_utc: dt.datetime) -> bool:
    """Check if current time is within regular market hours (9:30 AM - 4:00 PM ET)"""
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    hour = ny.hour
    minute = ny.minute
    # Market hours: 9:30 AM to 4:00 PM ET
    start_time = 9.5  # 9:30 AM
    end_time = 16.0   # 4:00 PM
    current_time = hour + minute / 60.0
    return start_time <= current_time <= end_time

def window_tag(now_utc: dt.datetime) -> str:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    return "am" if ny.hour < 12 else "pm"

def main(manual_trigger: bool = False):
    try:
        init_db(); init_settings_table()
    except Exception as e:
        insert_log("ERROR", "db_init_failed", {"err": str(e)})

    S = get_settings()
    trigger_type = "manual" if manual_trigger else "scheduled"
    insert_log("INFO", "run_start", {"enabled": S.get("ENABLED", True), "windows": S.get("WINDOWS_ET"), "trigger": trigger_type})

    if not S.get("ENABLED", True):
        insert_log("SKIP", "disabled", {}); return

    ok, info = is_market_open_now(buffer_min=S["AVOID_NEAR_OPEN_CLOSE_MIN"])
    if not ok:
        insert_log("SKIP", "market_closed_or_near_bell", info); return

    ny_date = info["now"].astimezone(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
    if S["MACRO_DATES"] and ny_date in set(S["MACRO_DATES"].split(",")):
        insert_log("SKIP", "macro_day", {"date": ny_date}); return

    now = info["now"]
    # Different time window logic for manual vs scheduled runs
    if manual_trigger:
        # For manual runs, allow trading during market hours
        if not within_market_hours_et(now):
            insert_log("SKIP", "outside_market_hours", {"now": now.isoformat(), "trigger": "manual"}); return
    else:
        # For scheduled runs, use exact time windows
        if not within_time_window_et(now, S["WINDOWS_ET"]):
            insert_log("SKIP", "outside_window", {"now": now.isoformat(), "trigger": "scheduled"}); return

    acct = get_account()
    equity = float(acct.portfolio_value); cash = float(acct.cash)
    insert_log("INFO", "account_snapshot", {"equity": equity, "cash": cash})

    positions = get_positions()
    universe = load_universe(S["UNIVERSE_MODE"])
    bars = get_bars(universe, days=250)
    if bars.empty:
        insert_log("ERROR", "no_bars", {}); return

    feats = compute_features(bars)
    invest_scalar = 1.0
    if S["REGIME_FILTER"]:
        breadth = compute_breadth(feats); spy = spy_regime(feats)
        if spy["spy_trend"] < 0.0 or breadth < 0.40: invest_scalar = float(S["RISK_OFF_SCALAR"])
        insert_log("INFO", "regime", {"breadth": round(breadth,3), "spy_trend": round(spy["spy_trend"],4), "scalar": invest_scalar})

    top = feats.head(50).copy()
    panel = top[["symbol","score","ret21","ret63","vol20_annual","trend","last","qual126"]].to_dict(orient="records")
    memctx = build_memory_context(5)
    rsp = choose_portfolio(json.dumps(panel), int(S["TARGET_POSITIONS"]), float(S["MAX_WEIGHT"]), model=os.getenv("MODEL_NAME","gpt-5-pro"), memory_context=memctx)
    insert_log("INFO", "picks_model", {"count": len(rsp.get("picks", []))})

    # earnings gating (only if configured)
    if S["EARNINGS_GATING"] and S.get("EARNINGS_PROVIDER") and S.get("EARNINGS_API_KEY") and rsp.get("picks"):
        try:
            from earnings_provider import get_upcoming_earnings
            cal = get_upcoming_earnings([p["symbol"] for p in rsp["picks"]])
            today = now.date(); keep = []
            before = int(S["EARNINGS_DAYS_BEFORE"]); after = int(S["EARNINGS_DAYS_AFTER"])
            for p in rsp["picks"]:
                d = cal.get(p["symbol"])
                if d is None or d < today - dt.timedelta(days=after) or d > today + dt.timedelta(days=before):
                    keep.append(p)
                else:
                    insert_log("SKIP", "blocked_by_earnings", {"symbol": p["symbol"], "earnings_date": str(d)})
            rsp["picks"] = keep
        except Exception as e:
            insert_log("WARNING", "earnings_gating_failed", {"err": str(e)})

    # weighting post-process
    if rsp.get("picks") and S.get("WEIGHTING_POSTPROCESS","vol_target") != "none":
        risk_w = risk_weights_for(rsp["picks"], top, float(S["MAX_WEIGHT"]))
        if risk_w:
            ai_w = {p["symbol"]: p["weight"] for p in rsp["picks"]}
            blend = {}
            AIW = float(S.get("AI_WEIGHT", 0.5))
            for sym in ai_w.keys() | risk_w.keys():
                w_ai = ai_w.get(sym, 0.0); w_r = risk_w.get(sym, 0.0)
                blend[sym] = min(float(S["MAX_WEIGHT"]), max(0.0, AIW*w_ai + (1.0-AIW)*w_r))
            ssum = sum(blend.values())
            if ssum > 1.0 and ssum > 0:
                for k in list(blend.keys()): blend[k] = blend[k]/ssum
            new_picks = []
            for p in rsp["picks"]:
                if p["symbol"] in blend and blend[p["symbol"]] > 0:
                    p["weight"] = blend[p["symbol"]]; new_picks.append(p)
            rsp["picks"] = new_picks

    # episode log
    episode_id = None
    try:
        constraints = {k: S[k] for k in ["TARGET_POSITIONS","MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","WINDOWS_ET","AVOID_NEAR_OPEN_CLOSE_MIN","USE_INTRADAY","REGIME_FILTER","RISK_OFF_SCALAR"] if k in S}
        episode_id = insert_episode(asof=now, window_tag=window_tag(now), equity=equity, cash=cash,
                                    notes=rsp.get("notes",""), confidence=float(rsp.get("confidence",0.5)),
                                    constraints=constraints, top_panel=panel)
        if rsp.get("picks"): insert_picks(episode_id, rsp["picks"])
    except Exception as e:
        insert_log("WARNING", "episode_log_failed", {"err": str(e)})

    # targets
    investable = max(0.0, equity * (1.0 - float(S["PORTFOLIO_CASH_BUFFER"]))) * invest_scalar
    targets = {p["symbol"]: investable * p["weight"] for p in rsp.get("picks", [])}

    last_px = {row["symbol"]: row["last"] for _, row in top.iterrows()}
    for sym in positions.keys():
        last_px.setdefault(sym, next((row["last"] for row in panel if row["symbol"]==sym), 0.0))

    if S["USE_INTRADAY"]:
        try:
            universe_for_px = set(list(targets.keys()) + list(positions.keys()))
            live_px = get_intraday_last_prices(universe_for_px, minutes=20)
            last_px.update(live_px)
        except Exception as e:
            insert_log("WARNING", "intraday_price_fetch_failed", {"err": str(e)})

    # turnover check
    cur_notional = sum(positions[s]["market_value"] for s in positions)
    est_turnover = 0.0
    for sym in set(list(positions.keys()) + list(targets.keys())):
        cur = (positions.get(sym, {}).get("market_value", 0.0))
        tgt = targets.get(sym, 0.0)
        est_turnover += abs(tgt - cur)
    if (cur_notional + cash) > 0 and est_turnover/(cur_notional + cash) > float(S["TURNOVER_LIMIT"]):
        insert_log("SKIP", "turnover_limit", {"est_turnover": est_turnover}); return

    fractionable = list_fractionable(list(set(list(targets.keys()) + list(positions.keys()))))
    orders = []
    for sym, tgt in targets.items():
        cur = positions.get(sym, {}).get("market_value", 0.0)
        delta = tgt - cur
        if abs(delta) < float(S["MIN_ORDER_NOTIONAL"]): continue
        side = "buy" if delta > 0 else "sell"
        if fractionable.get(sym, False):
            orders.append({"symbol": sym, "side": side, "notional": abs(delta)})
        else:
            px = last_px.get(sym, 0.0) or 1.0
            qty = int(abs(delta)/px)
            if qty > 0: orders.append({"symbol": sym, "side": side, "qty": qty})

    if orders:
        cancel_all_orders()
        if S.get("DRY_RUN", False):
            insert_log("INFO", "dry_run_orders", {"orders": orders})
        else:
            for od in orders:
                if "notional" in od: res = submit_notional_order(od["symbol"], od["notional"], od["side"])
                else: res = submit_qty_order(od["symbol"], od["qty"], od["side"])
                order_id = str(getattr(res, "id", ""))
                try:
                    if episode_id:
                        submitted_at = getattr(res, "submitted_at", None)
                        insert_order(episode_id, order_id, od["symbol"], od["side"], od.get("notional"), od.get("qty"), getattr(res,"status","submitted"), submitted_at)
                except Exception as e:
                    insert_log("WARNING", "order_log_failed", {"err": str(e)})
                insert_log("ORDER", "submitted", {"order_id": order_id, **od})
    else:
        insert_log("INFO", "no_orders", {})
