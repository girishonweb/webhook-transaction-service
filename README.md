# Transaction Webhook Service

Receives payment webhooks, acknowledges them instantly (`202`), and processes each transaction reliably in the background after a 30-second delay.

- **Live API:** https://webhook-transaction-service-td4w.onrender.com
- **Demo UI:** https://webhook-transaction-service-td4w.onrender.com/ui
- **Swagger docs:** https://webhook-transaction-service-td4w.onrender.com/docs

## How it works

```
Payment processor ──POST──> FastAPI ──INSERT (PROCESSING)──> Postgres ──> returns 202 (~100-300 ms)
                                                                  ^
Background worker (every 1s) ── finds rows older than 30s ────────┘
                                └─> sets status = PROCESSED, processed_at = now()
```

1. **Webhook arrives:** the payload is validated (Pydantic) and inserted with status `PROCESSING`. The API returns `202` right away. No processing happens in the request.
2. **Idempotency:** `transaction_id` is the primary key and the insert uses `ON CONFLICT DO NOTHING`. Duplicate or concurrent webhooks create exactly one record, with no errors.
3. **Background worker:** a loop started with the app polls Postgres every second for `PROCESSING` rows older than 30s and marks them `PROCESSED`. It uses `FOR UPDATE SKIP LOCKED`, so multiple workers or instances never process the same row twice.
4. **Reliability:** the database is the queue. If the server crashes or restarts mid-wait, pending rows are still in Postgres and get processed when the app is back. Worker errors are caught and logged, so the loop never dies.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/webhooks/transactions` | Accept a transaction webhook, returns `202 Accepted` |
| GET | `/` | Health check: `{"status":"HEALTHY","current_time":"..."}` |
| GET | `/v1/transactions/{transaction_id}` | Status (`PROCESSING` / `PROCESSED`), returns `404` if unknown |
| GET | `/ui` | Demo UI to send webhooks and track status |

## Test it

**Using the UI (`/ui`):**
- **Send webhook:** log shows `202` in under 500 ms, badge goes `PROCESSING`, then `PROCESSED` after ~30s
- **Send 5 duplicates:** five `202`s, but only one transaction is created
- **Track status:** paste any transaction ID to look it up

**Using curl:**
```bash
# 1. Send a webhook (expect 202)
curl -X POST https://webhook-transaction-service-td4w.onrender.com/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

# 2. Check status now (PROCESSING), and again after ~30s (PROCESSED)
curl https://webhook-transaction-service-td4w.onrender.com/v1/transactions/txn_abc123def456

# 3. Duplicate test: repeat step 1 several times; created_at never changes, only one row exists
```

**Edge cases:**
- Invalid body, e.g. `{"transaction_id":"x"}` returns `422` and the service keeps running
- Unknown ID returns `404`
- Restart the server while a transaction is `PROCESSING`: it is still processed after restart

## Run locally

```bash
git clone https://github.com/girishonweb/webhook-transaction-service.git
cd webhook-transaction-service
python -m venv .venv
.venv\Scripts\activate          # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file:
```
DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require
```

Create the table in your Postgres database:
```sql
CREATE TABLE transactions (
  transaction_id TEXT PRIMARY KEY,
  source_account TEXT NOT NULL,
  destination_account TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  currency TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PROCESSING',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ
);
CREATE INDEX idx_pending ON transactions (status, created_at);
```

Run:
```bash
python -m uvicorn main:app --reload
```
Open http://127.0.0.1:8000/ui

## Tech stack and why

| Tech | Why |
|---|---|
| **Python + FastAPI** | Async, fast, built-in validation and Swagger docs |
| **asyncpg** | Non-blocking Postgres driver, keeps the webhook under 500 ms |
| **Neon Postgres** (free, cloud) | Durable storage, transactions and row locking for the queue |
| **Pydantic** | Rejects malformed payloads with `422` before they touch the DB |
| **Render** (free tier) | Deploys from GitHub, runs a long-lived process (needed for the worker) |
| **UptimeRobot** | Pings `/` every 5 minutes so the free instance doesn't sleep |

## Trade-offs and next steps

- The worker polls every second. This is simple and reliable; at scale I would use a real queue (Redis/Celery, SQS) or Postgres `LISTEN/NOTIFY`.
- The 30s external API call is simulated by a delay before marking the row `PROCESSED`; real processing logic would plug into the same worker step, with retries and a `FAILED` status.
- On Render's free tier a cold start can take ~50s, so a keep-alive monitor is used.