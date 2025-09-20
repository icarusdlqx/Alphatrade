from __future__ import annotations
import os, json, datetime as dt, pytz
from typing import Any, Dict, List
from flask import Flask, render_template, request, redirect, url_for, session, flash
from openai import OpenAI

from settings_store import get_settings, set_settings, init_settings_table
from memory import init_db, fetch_orders, equity_series, recent_episodes
from alpaca_client import get_account, get_positions
from trader import main as run_trader

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET_KEY","alphatrade-dev-secret")

# ---------- simple password login ---------
def is_authed():
    return session.get("authed", False)

@app.before_request
def gate():
    open_paths = {"/login", "/static/"}
    if request.path.startswith("/static/"):
        return
    if request.path not in open_paths and not is_authed():
        return redirect(url_for("login"))

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        pwd = request.form.get("password","")
        if pwd and pwd == os.getenv("APP_PASSWORD","changeme"):
            session["authed"] = True
            return redirect(url_for("dashboard"))
        flash("Wrong password", "error")
    return render_template("login.html", app_name="AlphaTrade V3")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------- helpers ----------
def check_alpaca():
    try:
        acct = get_account()
        return True, f"Equity ${float(acct.portfolio_value):,.2f} | Cash ${float(acct.cash):,.2f}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def check_openai():
    try:
        key = os.getenv("OPENAI_API_KEY","")
        if not key: raise RuntimeError("Missing OPENAI_API_KEY")
        client = OpenAI(api_key=key)
        # Lightweight capability check
        models = client.models.list()
        names = [m.id for m in models.data[:5]]
        return True, "OK (" + ", ".join(names) + ")"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def next_windows_text(S: Dict[str,Any]) -> str:
    try:
        tz = pytz.timezone("America/New_York")
        now = dt.datetime.now(tz)
        times = [t.strip() for t in str(S.get("WINDOWS_ET","10:05,14:35")).split(",")]
        # naive "next" computation
        candidates = []
        for t in times:
            hh,mm = [int(x) for x in t.split(":")]
            d = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if d <= now: d = d + dt.timedelta(days=1)
            candidates.append(d)
        nxt = min(candidates)
        return nxt.strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        return "—"

# ---------- pages ----------
@app.route("/")
def root():
    return redirect(url_for("dashboard"))

@app.route("/dashboard")
def dashboard():
    # ensure DBs exist
    try: init_db()
    except Exception: pass
    try: init_settings_table()
    except Exception: pass

    S = get_settings()
    ok_alpaca, alpaca_msg = check_alpaca()
    ok_openai, openai_msg = check_openai()
    last_eps = recent_episodes(1)
    last_run = last_eps[0]["asof"].strftime("%Y-%m-%d %H:%M ET") if last_eps else "—"
    return render_template("dashboard.html",
                           app_name="AlphaTrade V3",
                           ok_alpaca=ok_alpaca, alpaca_msg=alpaca_msg,
                           ok_openai=ok_openai, openai_msg=openai_msg,
                           settings=S, last_run=last_run, next_run=next_windows_text(S))

@app.route("/run", methods=["POST"])
def run_now():
    run_trader()
    flash("Triggered analysis & rebalance. Check Trading History in a moment.", "info")
    return redirect(url_for("dashboard"))

@app.route("/positions")
def positions():
    ok, msg = check_alpaca()
    pos = {}
    if ok:
        pos = get_positions()
    return render_template("positions.html", app_name="AlphaTrade V3", positions=pos, status_msg=msg, ok=ok)

@app.route("/trades")
def trades():
    rows = fetch_orders(200)
    return render_template("trades.html", app_name="AlphaTrade V3", rows=rows)

@app.route("/performance")
def performance():
    series = equity_series(500)
    # Build arrays for Chart.js
    labels = [r["asof"].strftime("%Y-%m-%d %H:%M") for r in series]
    equity = [float(r["equity"]) for r in series]
    cash = [float(r["cash"]) for r in series]
    return render_template("performance.html", app_name="AlphaTrade V3", labels=labels, equity=equity, cash=cash)

@app.route("/settings", methods=["GET","POST"])
def settings():
    if request.method == "POST":
        payload = {}
        for key in [
            "TARGET_POSITIONS","MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","PORTFOLIO_CASH_BUFFER",
            "WINDOWS_ET","AVOID_NEAR_OPEN_CLOSE_MIN","UNIVERSE_MODE","USE_INTRADAY",
            "EARNINGS_GATING","EARNINGS_DAYS_BEFORE","EARNINGS_DAYS_AFTER","EARNINGS_PROVIDER","EARNINGS_API_KEY",
            "MACRO_DATES","REGIME_FILTER","RISK_OFF_SCALAR","WEIGHTING_POSTPROCESS","AI_WEIGHT","ENABLED"
        ]:
            v = request.form.get(key)
            if v is None: continue
            if key in {"USE_INTRADAY","EARNINGS_GATING","REGIME_FILTER","ENABLED"}:
                payload[key] = (v == "on")
            elif key in {"TARGET_POSITIONS","AVOID_NEAR_OPEN_CLOSE_MIN","EARNINGS_DAYS_BEFORE","EARNINGS_DAYS_AFTER"}:
                payload[key] = int(v) if v else 0
            elif key in {"MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","PORTFOLIO_CASH_BUFFER","RISK_OFF_SCALAR","AI_WEIGHT"}:
                payload[key] = float(v) if v else 0.0
            else:
                payload[key] = v
        set_settings(payload)
        flash("Settings saved.", "success")
        return redirect(url_for("settings"))
    S = get_settings()
    return render_template("settings.html", app_name="AlphaTrade V3", S=S)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
