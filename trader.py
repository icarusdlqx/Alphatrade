from __future__ import annotations
import os, json, math, pytz, datetime as dt, pathlib
import pandas as pd

from settings_store import get_settings, init_settings_table
from alpaca_client import (get_account, get_positions, get_bars, submit_notional_order, submit_qty_order, cancel_all_orders, is_market_open_now, list_fractionable, get_intraday_last_prices)
from strategy import compute_features, compute_breadth, spy_regime, risk_weights_for
from llm_policy import choose_portfolio
from memory import init_db, insert_episode, insert_picks, insert_order, build_memory_context
from earnings_provider import get_upcoming_earnings

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
    universe = sorted(set(sp500 + etfs))
    return universe

def within_time_window_et(now_utc: dt.datetime, windows_csv: str) -> bool:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    tstr = ny.strftime("%H:%M")
    return tstr in set(w.strip() for w in windows_csv.split(","))

def window_tag(now_utc: dt.datetime) -> str:
    ny = now_utc.astimezone(pytz.timezone("America/New_York"))
    return "am" if ny.hour < 12 else "pm"

def human_summary(rsp: dict) -> str:
    lines = [f"Run @ {rsp.get('asof')}"]
    if not rsp.get("picks"):
        lines.append("No changes recommended."); return "\n".join(lines)
    for p in rsp["picks"]:
        lines.append(f"- {p['symbol']}: {p['weight']:.2%} — {p['rationale']}")
    if rsp.get("notes"): lines.append(f"Notes: {rsp['notes']}")
    return "\n".join(lines)

def compute_targets(investable_equity: float, picks: list[dict]) -> dict[str, float]:
    notional_targets = {}
    total_weight = sum(p["weight"] for p in picks)
    if total_weight == 0: return {}
    scale = min(1.0, 1.0/total_weight)
    for p in picks:
        w = p["weight"]*scale
        notional_targets[p["symbol"]] = investable_equity * w
    return notional_targets

def diff_to_orders(prices: dict[str,float], current_positions: dict[str,dict], targets_notional: dict[str,float], min_order_notional: float, fractionable: dict[str,bool]) -> list[dict]:
    cur_notional = {sym: pos["qty"]*prices.get(sym, 0.0) for sym, pos in current_positions.items()}
    symbols = set(list(targets_notional.keys()) + list(cur_notional.keys()))
    orders = []
    for sym in symbols:
        tgt = targets_notional.get(sym, 0.0); cur = cur_notional.get(sym, 0.0)
        delta = tgt - cur
        if abs(delta) < min_order_notional: continue
        side = "buy" if delta > 0 else "sell"
        if fractionable.get(sym, False):
            orders.append({"symbol": sym, "side": side, "notional": abs(delta)})
        else:
            px = prices.get(sym, 0.0) or 1.0
            qty = int(abs(delta)/px)
            if qty > 0: orders.append({"symbol": sym, "side": side, "qty": qty})
    return orders

def main():
    # Init DBs
    try: init_db()
    except Exception as e: print({"warning":"db_init_failed","error":str(e)})
    try: init_settings_table()
    except Exception as e: print({"warning":"settings_init_failed","error":str(e)})

    S = get_settings()
    if not S.get("ENABLED", True):
        print({"skipped":"trader_disabled"}); return

    ok, info = is_market_open_now(buffer_min=S["AVOID_NEAR_OPEN_CLOSE_MIN"])
    if not ok:
        print({"skipped":"market closed or too near open/close", **info}); return

    ny_date = info["now"].astimezone(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")
    if S["MACRO_DATES"] and ny_date in set(S["MACRO_DATES"].split(",")):
        print({"skipped":"macro_event_day", "date": ny_date}); return

    now = info["now"]
    if not within_time_window_et(now, S["WINDOWS_ET"]):
        print({"skipped":"outside configured windows", "now": now.isoformat()}); return

    acct = get_account()
    equity = float(acct.portfolio_value); cash = float(acct.cash)
    investable_equity = max(0.0, equity * (1.0 - float(S["PORTFOLIO_CASH_BUFFER"])))

    positions = get_positions()
    universe = load_universe(S["UNIVERSE_MODE"])
    bars = get_bars(universe, days=250)
    if bars.empty: print({"error":"no bars returned"}); return

    feats = compute_features(bars)
    invest_scalar = 1.0
    if S["REGIME_FILTER"]:
        breadth = compute_breadth(feats); spy = spy_regime(feats)
        if spy["spy_trend"] < 0.0 or breadth < 0.40: invest_scalar = float(S["RISK_OFF_SCALAR"])
        print({"regime":{"breadth": round(breadth,3), "spy_trend": round(spy["spy_trend"],4), "scalar": invest_scalar}})

    top = feats.head(50).copy()
    panel = top[["symbol","score","ret21","ret63","vol20_annual","trend","last","qual126"]].to_dict(orient="records")
    memctx = build_memory_context(5)
    rsp = choose_portfolio(json.dumps(panel), int(S["TARGET_POSITIONS"]), float(S["MAX_WEIGHT"]), model=S.get("MODEL_NAME", "gpt-5"), memory_context=memctx)

    # Earnings gating (only if configured)
    if S["EARNINGS_GATING"] and S.get("EARNINGS_PROVIDER") and S.get("EARNINGS_API_KEY") and rsp.get("picks"):
        try:
            cal = get_upcoming_earnings([p["symbol"] for p in rsp["picks"]])
            today = now.date(); keep = []
            before = int(S["EARNINGS_DAYS_BEFORE"]); after = int(S["EARNINGS_DAYS_AFTER"])
            for p in rsp["picks"]:
                d = cal.get(p["symbol"])
                if d is None or d < today - dt.timedelta(days=after) or d > today + dt.timedelta(days=before):
                    keep.append(p)
                else:
                    print({"info":"blocked_by_earnings", "symbol": p["symbol"], "earnings_date": str(d)})
            rsp["picks"] = keep
        except Exception as e:
            print({"warning":"earnings_gating_failed","error":str(e)})

    # Weighting post-process (vol-target blend)
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

    # Episode log (best-effort)
    episode_id = None
    try:
        constraints = {k: S[k] for k in ["TARGET_POSITIONS","MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","WINDOWS_ET","AVOID_NEAR_OPEN_CLOSE_MIN","USE_INTRADAY","REGIME_FILTER","RISK_OFF_SCALAR"] if k in S}
        episode_id = insert_episode(asof=now, window_tag=window_tag(now), equity=equity, cash=cash,
                                    notes=rsp.get("notes",""), confidence=float(rsp.get("confidence",0.5)),
                                    constraints=constraints, top_panel=panel)
        if rsp.get("picks"): insert_picks(episode_id, rsp["picks"])
    except Exception as e:
        print({"warning":"episode_log_failed","error":str(e)})

    investable = investable_equity * invest_scalar
    targets = compute_targets(investable, rsp.get("picks", []))

    last_px = {row["symbol"]: row["last"] for _, row in top.iterrows()}
    for sym in positions.keys():
        last_px.setdefault(sym, next((row["last"] for row in panel if row["symbol"]==sym), 0.0))

    if S["USE_INTRADAY"]:
        try:
            universe_for_px = set(list(targets.keys()) + list(positions.keys()))
            live_px = get_intraday_last_prices(universe_for_px, minutes=20)
            last_px.update(live_px)
        except Exception as e:
            print({"warning":"intraday_price_fetch_failed","error":str(e)})

    cur_notional = sum(positions[s]["market_value"] for s in positions)
    est_turnover = 0.0
    for sym in set(list(positions.keys()) + list(targets.keys())):
        cur = (positions.get(sym, {}).get("market_value", 0.0))
        tgt = targets.get(sym, 0.0)
        est_turnover += abs(tgt - cur)
    if (cur_notional + cash) > 0 and est_turnover/(cur_notional + cash) > float(S["TURNOVER_LIMIT"]):
        print({"skipped":"turnover limit", "est_turnover": est_turnover}); return

    fractionable = list_fractionable(list(set(list(targets.keys()) + list(positions.keys()))))
    orders = diff_to_orders(last_px, positions, targets, float(S["MIN_ORDER_NOTIONAL"]), fractionable)

    if orders:
        cancel_all_orders()
        if S.get("DRY_RUN", False):
            print({"dry_run_orders": orders})
        else:
            from memory import insert_order
            for od in orders:
                if "notional" in od: res = submit_notional_order(od["symbol"], od["notional"], od["side"])
                else: res = submit_qty_order(od["symbol"], od["qty"], od["side"])
                order_id = str(getattr(res, "id", ""))
                print({"submitted": od, "order_id": order_id})
                try:
                    if episode_id:
                        submitted_at = getattr(res, "submitted_at", None)
                        insert_order(episode_id, order_id, od["symbol"], od["side"], od.get("notional"), od.get("qty"), getattr(res,"status","submitted"), submitted_at)
                except Exception as e:
                    print({"warning":"order_log_failed","error":str(e)})
    else:
        print({"info":"no orders necessary"})

if __name__ == "__main__":
    main()
