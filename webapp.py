from __future__ import annotations
import os, json, datetime as dt, pytz
from typing import Any, Dict
from flask import Flask, render_template, request, redirect, url_for, session, flash
from openai import OpenAI

from settings_store import get_settings, set_settings, init_settings_table
from memory import init_db, fetch_logs, equity_series, recent_episodes
from alpaca_client import get_account, get_positions
from trader import main as run_trader

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET_KEY","alphatrade-dev-secret")

def to_et(dtobj):
    """Safely convert a datetime object to Eastern Time"""
    if not dtobj:
        return None
    ET = pytz.timezone("America/New_York")
    if getattr(dtobj, 'tzinfo', None) is None:
        # Assume naive datetime is UTC
        dtobj = pytz.utc.localize(dtobj)
    return dtobj.astimezone(ET)

def startup_health_check():
    """Comprehensive startup health check to validate all critical components"""
    health_status = {"database": False, "secrets": False, "alpaca": False, "openai": False}
    issues = []
    
    # Database connectivity check
    try:
        from memory import _conn
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        health_status["database"] = True
        app.logger.info("✓ Database connection successful")
    except Exception as e:
        issues.append(f"Database connection failed: {e}")
        app.logger.error(f"✗ Database connection failed: {e}")
    
    # Required secrets validation
    required_secrets = {
        "APP_PASSWORD": os.getenv("APP_PASSWORD"),
        "ALPACA_API_KEY": os.getenv("ALPACA_API_KEY"),
        "ALPACA_SECRET_KEY": os.getenv("ALPACA_SECRET_KEY"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY")
    }
    
    missing_secrets = [key for key, value in required_secrets.items() if not value or value == "changeme"]
    if missing_secrets:
        issues.append(f"Missing or default secrets: {', '.join(missing_secrets)}")
        app.logger.warning(f"⚠ Missing or default secrets: {', '.join(missing_secrets)}")
    else:
        health_status["secrets"] = True
        app.logger.info("✓ All required secrets configured")
    
    # Alpaca API connectivity check
    try:
        ok_alpaca, alpaca_msg = check_alpaca()
        if ok_alpaca:
            health_status["alpaca"] = True
            app.logger.info(f"✓ Alpaca API connection successful: {alpaca_msg}")
        else:
            issues.append(f"Alpaca API check failed: {alpaca_msg}")
            app.logger.warning(f"⚠ Alpaca API check failed: {alpaca_msg}")
    except Exception as e:
        issues.append(f"Alpaca API validation error: {e}")
        app.logger.error(f"✗ Alpaca API validation error: {e}")
    
    # OpenAI API connectivity check
    try:
        ok_openai, openai_msg = check_openai()
        if ok_openai:
            health_status["openai"] = True
            app.logger.info(f"✓ OpenAI API connection successful: {openai_msg}")
        else:
            issues.append(f"OpenAI API check failed: {openai_msg}")
            app.logger.warning(f"⚠ OpenAI API check failed: {openai_msg}")
    except Exception as e:
        issues.append(f"OpenAI API validation error: {e}")
        app.logger.error(f"✗ OpenAI API validation error: {e}")
    
    # Summary
    if issues:
        app.logger.warning(f"Startup health check completed with {len(issues)} issues: {'; '.join(issues)}")
    else:
        app.logger.info("✓ Startup health check completed successfully - all systems operational")
    
    return health_status, issues

def is_authed():
    return session.get("authed", False)

@app.before_request
def gate():
    if request.path.startswith("/static/") or request.path in {"/login"}:
        return
    if not is_authed():
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

def check_alpaca():
    # Check for required Alpaca environment variables first
    alpaca_key = os.getenv("ALPACA_API_KEY")
    alpaca_secret = os.getenv("ALPACA_SECRET_KEY")
    
    if not alpaca_key or not alpaca_secret:
        missing = []
        if not alpaca_key: missing.append("ALPACA_API_KEY")
        if not alpaca_secret: missing.append("ALPACA_SECRET_KEY")
        return False, f"Missing required secrets: {', '.join(missing)}"
    
    try:
        acct = get_account()
        # Safely handle account object properties using getattr with defaults
        portfolio_val = float(getattr(acct, 'portfolio_value', 0) or 0)
        cash_val = float(getattr(acct, 'cash', 0) or 0)
        # Equity is portfolio value minus cash (since portfolio_value includes cash)
        equity_val = portfolio_val - cash_val
        return True, f"Equity ${equity_val:,.2f} | Cash ${cash_val:,.2f}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def check_openai():
    # Check for required OpenAI API key first
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return False, "Missing required secret: OPENAI_API_KEY"
    
    try:
        client = OpenAI(api_key=key)
        # Test the connection by calling the API
        models = client.models.list()
        # Return status with configured model information
        configured_model = os.getenv("MODEL_NAME", "gpt-5")
        return True, f"OK (using {configured_model} with medium reasoning effort)"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def next_windows_text(S: Dict[str,Any]) -> str:
    try:
        tz = pytz.timezone("America/New_York")
        now = dt.datetime.now(tz)
        times = [t.strip() for t in str(S.get("WINDOWS_ET","10:05,14:35")).split(",")]
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

@app.route("/")
def root():
    return redirect(url_for("dashboard"))

@app.route("/dashboard")
def dashboard():
    # Database initialization with proper error logging
    try:
        init_db()
        app.logger.info("Database initialized successfully")
    except Exception as e:
        app.logger.error(f"Database initialization failed: {type(e).__name__}: {e}")
        flash("Database connection issue detected. Please check configuration.", "error")
    
    try:
        init_settings_table()
        app.logger.info("Settings table initialized successfully")
    except Exception as e:
        app.logger.error(f"Settings table initialization failed: {type(e).__name__}: {e}")
        flash("Settings initialization issue detected.", "warning")

    S = get_settings()
    ok_alpaca, alpaca_msg = check_alpaca()
    ok_openai, openai_msg = check_openai()
    last_eps = recent_episodes(1)
    if last_eps:
        # Convert UTC timestamp to Eastern Time for display
        last_asof_et = to_et(last_eps[0]["asof"])
        last_run = last_asof_et.strftime("%Y-%m-%d %H:%M ET") if last_asof_et else "—"
    else:
        last_run = "—"
    return render_template("dashboard.html",
                           app_name="AlphaTrade V3",
                           ok_alpaca=ok_alpaca, alpaca_msg=alpaca_msg,
                           ok_openai=ok_openai, openai_msg=openai_msg,
                           settings=S, last_run=last_run, next_run=next_windows_text(S))

@app.route("/run", methods=["POST"])
def run_now():
    run_trader(manual_trigger=True)
    flash("Triggered analysis & rebalance. Check Log for details.", "info")
    return redirect(url_for("dashboard"))

@app.route("/positions")
def positions():
    ok, msg = check_alpaca()
    pos = {}
    if ok: pos = get_positions()
    return render_template("positions.html", app_name="AlphaTrade V3", positions=pos, status_msg=msg, ok=ok)

@app.route("/log")
def log():
    import json
    rows = fetch_logs(400)
    # Parse JSON details for template and convert timestamps to ET
    for row in rows:
        try:
            if isinstance(row['detail'], (dict, list)):
                row['parsed_detail'] = row['detail']
            elif isinstance(row['detail'], (str, bytes)):
                row['parsed_detail'] = json.loads(row['detail'])
            else:
                row['parsed_detail'] = {}
        except:
            row['parsed_detail'] = {}
        
        # Convert UTC timestamp to Eastern Time and pre-format for template
        at_et = to_et(row['at'])
        row['at_et_str'] = at_et.strftime('%m/%d %H:%M:%S ET') if at_et else '—'
    return render_template("log.html", app_name="AlphaTrade V3", rows=rows)

@app.route("/performance")
def performance():
    from memory import equity_series
    series = equity_series(500)
    # Convert UTC timestamps to Eastern Time for performance chart labels
    labels = [to_et(r["asof"]).strftime("%Y-%m-%d %H:%M ET") if to_et(r["asof"]) else str(r["asof"]) for r in series]
    equity = [float(r["equity"]) for r in series]
    cash = [float(r["cash"]) for r in series]
    # Calculate total (equity + cash) for each data point
    total = [e + c for e, c in zip(equity, cash)]
    return render_template("performance.html", app_name="AlphaTrade V3", labels=labels, equity=equity, cash=cash, total=total)

@app.route("/settings", methods=["GET","POST"])
def settings():
    if request.method == "POST":
        payload = {}
        for key in [
            "ENABLED","TARGET_POSITIONS","MAX_WEIGHT","TURNOVER_LIMIT","MIN_ORDER_NOTIONAL","PORTFOLIO_CASH_BUFFER",
            "WINDOWS_ET","AVOID_NEAR_OPEN_CLOSE_MIN","UNIVERSE_MODE","USE_INTRADAY",
            "EARNINGS_GATING","EARNINGS_DAYS_BEFORE","EARNINGS_DAYS_AFTER","EARNINGS_PROVIDER","EARNINGS_API_KEY",
            "MACRO_DATES","REGIME_FILTER","RISK_OFF_SCALAR","WEIGHTING_POSTPROCESS","AI_WEIGHT"
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
        from settings_store import set_settings
        set_settings(payload)
        flash("Settings saved.", "success")
        return redirect(url_for("settings"))
    from settings_store import get_settings
    S = get_settings()
    return render_template("settings.html", app_name="AlphaTrade V3", S=S)

if __name__ == "__main__":
    # Perform startup health check
    print("Performing startup health check...")
    health_status, issues = startup_health_check()
    
    if issues:
        print(f"⚠ Startup completed with {len(issues)} issues - check logs for details")
        for issue in issues[:3]:  # Show first 3 issues
            print(f"  - {issue}")
        if len(issues) > 3:
            print(f"  ... and {len(issues) - 3} more issues (check logs)")
    else:
        print("✓ All startup health checks passed")
    
    app.run(host="0.0.0.0", port=5000, debug=True)
