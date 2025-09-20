# AlphaTrade V3 (Replit)

A twice‑daily GPT‑5 Pro + Alpaca trader with a simple web dashboard.

## What you get
- Login gate (single password via `APP_PASSWORD`)
- Dashboard with **API status** (Alpaca + OpenAI), last/next analysis time, and run buttons
- **Settings** page with tunable "levers" (saved in DB, override env at runtime)
- **Positions** page (live from Alpaca)
- **Trading History** from internal DB (`orders` table)
- **Performance** from DB `episodes` (equity timeseries) vs. SPY
- Backend trading job with **intraday price overlay**, **earnings gating (stub)**, **macro-date throttle**, **regime filter**, **vol-targeted weights**, and **memory**

## One‑time setup
1) Create a Python Repl, upload this folder (or the zip), and set **Secrets**:
   - `APP_PASSWORD` (login password)
   - `APP_SECRET_KEY` (any random string for Flask sessions)
   - `OPENAI_API_KEY`
   - `ALPACA_API_KEY_V3`, `ALPACA_SECRET_KEY_V3`
   - `APCA_BASE_URL` = `https://paper-api.alpaca.markets` (paper first)
   - `DB_URL` (Replit: Tools → Database → copy connection string)

2) Install + run:
   ```bash
   pip install -r requirements.txt
   # Web UI:
   gunicorn -w 1 -b 0.0.0.0:8000 webapp:app
   # Trader (manual run):
   python trader.py
   ```

## Scheduled runs
Create two **Scheduled Deployments** to run `python trader.py` at **10:05 ET** and **14:35 ET**.

## Notes
- Settings saved in DB override env defaults for the trader.
- This is technical software, not financial advice. Paper‑trade first.
