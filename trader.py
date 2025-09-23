from __future__ import annotations
import os, json, pytz, datetime as dt, pathlib
import pandas as pd

from settings_store import get_settings, init_settings_table
from alpaca_client import (get_account, get_positions, get_bars, submit_notional_order, submit_qty_order, cancel_all_orders, is_market_open_now, list_fractionable, get_intraday_last_prices)
from strategy import compute_features, compute_breadth, spy_regime, risk_weights_for
from llm_policy import choose_portfolio
from memory import init_db, insert_episode, insert_picks, insert_order, build_memory_context, insert_log

DATA_DIR = pathlib.Path(__file__).parent / "data"

def load_universe(mode: str = "sp500_etfs") -> list[str]:
    etfs = [s.strip() for s in open(DATA_DIR/"etfs_large.csv").read().strip().splitlines() if s.strip()]
    if mode == "etfs_only":
        return etfs
    sp500 = []
    try:
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
        sp500 = list(pd.read_csv(url)["Symbol"].dropna().astype(str).str.upper().unique())
    except Exception:
        sp500 = [s.strip() for s in open(DATA_DIR/"sp500_fallback.csv").read().strip().splitlines() if s.strip()]
    return sorted(set(sp500 + etfs))

def within_time_window_et(now_utc: dt.datetime, windows_csv: str, tol_min: int) -> bool:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    times = [t.strip() for t in windows_csv.split(",") if t.strip()]
    for t in times:
        hh, mm = [int(x) for x in t.split(":")]
        target = ny.replace(hour=hh, minute=mm, second=0, microsecond=0)
        delta = abs((ny - target).total_seconds())/60.0
        if delta <= tol_min:
            return True
    return False

def window_tag(now_utc: dt.datetime) -> str:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    return "am" if ny.hour < 12 else "pm"

def _equity_fallback(acct):
    # Some accounts occasionally return 0 for portfolio_value in paper mode.
    for field in ("portfolio_value", "equity", "cash"):
        try:
            val = float(getattr(acct, field))
            if val and val > 0:
                return val
        except Exception:
            continue
    return 0.0

def main(force: bool=False, trigger: str="manual"):
    try: init_db(); init_settings_table()
    except Exception as e: insert_log("ERROR", "db_init_failed", {"err": str(e)})

    S = get_settings()
    insert_log("INFO", "run_start", {"enabled": S.get("ENABLED", True), "trigger": trigger, "windows": S.get("WINDOWS_ET")})

    if not S.get("ENABLED", True):
        insert_log("SKIP", "disabled", {}); return

    ok, info = is_market_open_now(buffer_min=S["AVOID_NEAR_OPEN_CLOSE_MIN"])
    if not ok:
        insert_log("SKIP", "market_closed_or_near_bell", info); return

    # Use Eastern Time for all timestamps instead of UTC
    now = dt.datetime.now(pytz.timezone("America/New_York"))

    if not force and not within_time_window_et(now.astimezone(pytz.UTC), S["WINDOWS_ET"], int(S.get("WINDOW_TOL_MIN",30))):
        insert_log("SKIP", "outside_window", {"now": now.isoformat()}); return

    # Sync portfolio positions from Alpaca (source of truth)
    insert_log("INFO", "sync_start", {"message": "Synchronizing portfolio from Alpaca account (source of truth)"})
    
    try:
        acct = get_account()
        # Get total portfolio value and cash from Alpaca (not buying power with leverage)
        total_portfolio = _equity_fallback(acct)  # Uses portfolio_value, equity, or cash as fallback
        cash = float(getattr(acct, "cash", 0.0))
        
        # Sanity check for Alpaca inconsistencies 
        if total_portfolio == 0 and cash > 0:
            total_portfolio = cash
            insert_log("WARNING", "alpaca_pv_inconsistency", {"message": "Portfolio value zero but cash available, using cash as total portfolio", "cash": cash})
        
        equity = max(0.0, total_portfolio - cash)  # Market value of positions only
        
        if equity <= 0 and cash <= 0:
            insert_log("ERROR", "sync_failed", {"error": "Account shows zero equity and cash - sync failed"})
            return
        
        insert_log("INFO", "sync_success", {
            "total_portfolio": total_portfolio,
            "cash": cash,
            "equity": equity,
            "message": "Portfolio successfully synchronized from Alpaca"
        })
        
    except Exception as e:
        insert_log("ERROR", "alpaca_sync_failed", {"error": str(e), "message": "Failed to sync with Alpaca - trading aborted"})
        return

    universe = load_universe(S["UNIVERSE_MODE"])
    bars = get_bars(universe, days=250)
    if bars.empty: insert_log("ERROR", "no_bars", {}); return

    feats = compute_features(bars)
    invest_scalar = 1.0
    breadth = compute_breadth(feats)
    spy = spy_regime(feats)
    if S["REGIME_FILTER"]:
        if spy["spy_trend"] < 0.0 or breadth < 0.40: invest_scalar = float(S["RISK_OFF_SCALAR"])

    spy_price_intraday = None
    try:
        if S["USE_INTRADAY"]:
            px = get_intraday_last_prices({"SPY"}, minutes=30)
            if px.get("SPY"):
                last_close = float(bars.xs("SPY", level="symbol")["close"].iloc[-1])
                spy_price_intraday = (px["SPY"]/last_close - 1.0) if last_close else None
    except Exception:
        pass

    insert_log("INFO", "regime_detail", {
        "breadth_pct": round(100*breadth,1),
        "spy_trend_pct": round(100*spy.get("spy_trend", 0.0),1),
        "risk_scalar_pct": round(100*invest_scalar,1),
        "spy_intraday_pct": round(100*spy_price_intraday,2) if spy_price_intraday is not None else None,
        "note": "Regime uses daily trend; it changes mainly on new daily bars."
    })

    # AI portfolio
    top = feats.head(50).copy()
    panel = top[["symbol","score","ret21","ret63","vol20_annual","trend","last","qual126"]].to_dict(orient="records")
    memctx = build_memory_context(5)
    rsp = choose_portfolio(json.dumps(panel), int(S["TARGET_POSITIONS"]), float(S["MAX_WEIGHT"]), model=os.getenv("MODEL_NAME","gpt-5"), memory_context=memctx)
    picks = rsp.get("picks", [])
    insert_log("INFO", "picks_model", {"count": len(picks)})
    if picks:
        insert_log("INFO", "picks_detail", [{"symbol": p["symbol"], "w": p["weight"], "why": p.get("rationale","")} for p in picks])

    # Weighting and gating
    if picks and S.get("WEIGHTING_POSTPROCESS","vol_target") != "none":
        from strategy import risk_weights_for
        risk_w = risk_weights_for(picks, top, float(S["MAX_WEIGHT"]))
        if risk_w:
            ai_w = {p["symbol"]: p["weight"] for p in picks}
            blend = {}
            AIW = float(S.get("AI_WEIGHT", 0.5))
            for sym in ai_w.keys() | risk_w.keys():
                blend[sym] = min(float(S["MAX_WEIGHT"]), max(0.0, AIW*ai_w.get(sym,0.0) + (1.0-AIW)*risk_w.get(sym,0.0)))
            ssum = sum(blend.values())
            if ssum > 1.0 and ssum > 0:
                for k in list(blend.keys()): blend[k] = blend[k]/ssum
            new_picks = []
            for p in picks:
                if p["symbol"] in blend and blend[p["symbol"]] > 0:
                    p["weight"] = blend[p["symbol"]]; new_picks.append(p)
            picks = new_picks

    # Record episode with synchronized Alpaca portfolio data before making trades
    insert_log("INFO", "episode_recording", {"message": "Recording episode with Alpaca-synchronized portfolio data"})
    episode_id = None
    try:
        constraints = {k: S[k] for k in ["TARGET_POSITIONS","MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","WINDOWS_ET","WINDOW_TOL_MIN","AVOID_NEAR_OPEN_CLOSE_MIN","USE_INTRADAY","REGIME_FILTER","RISK_OFF_SCALAR"] if k in S}
        episode_id = insert_episode(asof=now, window_tag=("am" if now.astimezone(pytz.timezone("America/New_York")).hour<12 else "pm"),
                                    equity=equity, cash=cash, notes=rsp.get("notes",""), confidence=float(rsp.get("confidence",0.5)),
                                    constraints=constraints, top_panel=panel)
        if picks: insert_picks(episode_id, picks)
    except Exception as e:
        insert_log("WARNING", "episode_log_failed", {"err": str(e)})

    # Targets and orders - use total portfolio value, not just existing positions
    investable = max(0.0, total_portfolio * (1.0 - float(S["PORTFOLIO_CASH_BUFFER"]))) * invest_scalar
    targets = {p["symbol"]: investable * p["weight"] for p in picks}

    positions = get_positions()
    cur_invested = sum(positions[s]["market_value"] for s in positions)

    # ----- Turnover gate: compare to CURRENT INVESTED, skip gate if building from scratch
    est_turnover = 0.0
    for sym in set(list(positions.keys()) + list(targets.keys())):
        cur = positions.get(sym, {}).get("market_value", 0.0)
        tgt = targets.get(sym, 0.0)
        est_turnover += abs(tgt - cur)

    if cur_invested > float(S["MIN_ORDER_NOTIONAL"]) * 2:
        denom = cur_invested
        if denom > 0 and est_turnover/denom > float(S["TURNOVER_LIMIT"]):
            insert_log("SKIP", "turnover_limit", {"est_turnover": est_turnover, "denom": denom})
            insert_log("INFO", "analysis_summary", {"decision": "no_orders", "reason": "Turnover cap exceeded for existing portfolio."})
            return
    # ----- end turnover gate

    # Price map
    last_px = {row["symbol"]: row["last"] for _, row in pd.DataFrame(panel).iterrows() if "symbol" in row}
    if S["USE_INTRADAY"]:
        try:
            universe_for_px = set(list(targets.keys()) + list(positions.keys()))
            live_px = get_intraday_last_prices(universe_for_px, minutes=20)
            last_px.update(live_px)
        except Exception as e:
            insert_log("WARNING", "intraday_price_fetch_failed", {"err": str(e)})

    # Build orders
    fractionable = list_fractionable(list(set(list(targets.keys()) + list(positions.keys()))))
    orders = []
    for sym, tgt in targets.items():
        cur = positions.get(sym, {}).get("market_value", 0.0)
        delta = tgt - cur
        if abs(delta) < float(S["MIN_ORDER_NOTIONAL"]): 
            insert_log("SKIP", "dust_trade_skipped", {"symbol": sym, "delta": delta})
            continue
        side = "buy" if delta > 0 else "sell"
        if fractionable.get(sym, False):
            orders.append({"symbol": sym, "side": side, "notional": abs(delta)})
        else:
            px = last_px.get(sym, 0.0) or 1.0
            qty = int(abs(delta)/px)
            if qty > 0: orders.append({"symbol": sym, "side": side, "qty": qty})

    if not picks:
        insert_log("INFO", "analysis_summary", {"decision": "no_new_positions", "reason": "AI did not find sufficiently strong candidates."})
        return

    if not orders:
        insert_log("INFO", "analysis_summary", {"decision": "no_orders", "reason": "Targets close to current holdings, dust threshold, or zero investable equity."})
        return

    cancel_all_orders()
    if S.get("DRY_RUN", False):
        insert_log("INFO", "dry_run_orders", {"orders": orders})
        insert_log("INFO", "analysis_summary", {"decision": "dry_run_only", "orders": orders})
        return

    submitted_order_ids = []
    for od in orders:
        if "notional" in od: res = submit_notional_order(od["symbol"], od["notional"], od["side"])
        else: res = submit_qty_order(od["symbol"], od["qty"], od["side"])
        order_id = str(getattr(res, "id", ""))
        submitted_order_ids.append(order_id)
        try:
            if episode_id:
                submitted_at = getattr(res, "submitted_at", None)
                insert_order(episode_id, order_id, od["symbol"], od["side"], od.get("notional"), od.get("qty"), getattr(res,"status","submitted"), submitted_at)
        except Exception as e:
            insert_log("WARNING", "order_log_failed", {"err": str(e)})
        insert_log("ORDER", "submitted", {"order_id": order_id, **od})

    # Order reconciliation - verify orders were executed correctly
    import time
    if submitted_order_ids:
        insert_log("INFO", "reconciliation_start", {"order_count": len(submitted_order_ids)})
        time.sleep(2)  # Brief delay to allow orders to be processed
        
        try:
            from alpaca_client import reconcile_orders
            reconciliation_results = reconcile_orders(submitted_order_ids)
            
            filled_orders = 0
            failed_orders = 0
            for order_id, result in reconciliation_results.items():
                if "error" in result:
                    insert_log("ERROR", "order_reconciliation_error", {"order_id": order_id, "error": result["error"]})
                    failed_orders += 1
                elif result.get("status") in ["filled", "partially_filled"]:
                    insert_log("INFO", "order_filled", {
                        "order_id": order_id,
                        "status": result["status"],
                        "symbol": result["symbol"],
                        "side": result["side"],
                        "filled_qty": result.get("filled_qty"),
                        "filled_price": result.get("filled_avg_price")
                    })
                    filled_orders += 1
                else:
                    insert_log("WARNING", "order_pending", {
                        "order_id": order_id,
                        "status": result["status"],
                        "symbol": result["symbol"]
                    })
            
            insert_log("INFO", "reconciliation_complete", {
                "total_orders": len(submitted_order_ids),
                "filled_orders": filled_orders,
                "failed_orders": failed_orders,
                "pending_orders": len(submitted_order_ids) - filled_orders - failed_orders
            })
            
        except Exception as e:
            insert_log("ERROR", "reconciliation_failed", {"error": str(e)})

    insert_log("INFO", "analysis_summary", {"decision": "orders_submitted", "count": len(orders)} )