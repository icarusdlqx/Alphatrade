# AlphaTrade V3 (Log Edition)

- Adds a unified **Log** page (replaces "Trading History").
- Every run writes a **run log**: start, checks, skips (market closed / macro day / outside window / turnover), regime metrics, picks summary, and **trades** (highlighted).
- Orders are still stored in the `orders` table; the Log page is a superset view.

## Secrets
APP_PASSWORD, APP_SECRET_KEY, OPENAI_API_KEY, ALPACA_API_KEY_V3, ALPACA_SECRET_KEY_V3, APCA_BASE_URL, DB_URL, [optional] DRY_RUN

## Run
```bash
pip install -r requirements.txt
gunicorn -w 1 -b 0.0.0.0:8000 webapp:app
# trader (manual)
python trader.py
```
