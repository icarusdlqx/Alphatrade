# AlphaTrade V3 — GPT‑5 + Log + Window Tolerance

- GPT‑5 Responses API with `reasoning={"effort":"medium"}`.
- Unified **Log** page (replaces Trading History): run starts, skips, regime metrics, picks count, and **submitted orders in bold**.
- **Window tolerance**: treat runs within ±**30 min** (configurable) of each window as valid; **Run Now** bypasses windows entirely.
- Twice‑daily schedule via Replit Automations still recommended.

## Secrets
APP_PASSWORD, APP_SECRET_KEY, OPENAI_API_KEY, ALPACA_API_KEY_V3, ALPACA_SECRET_KEY_V3, APCA_BASE_URL, DB_URL, [optional] DRY_RUN, MODEL_NAME, REASONING_EFFORT

## Install/Run
pip install -r requirements.txt
gunicorn -w 1 -b 0.0.0.0:8000 webapp:app
