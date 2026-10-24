import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
PROCESS_DELAY_SECONDS = 30.0


class TransactionWebhook(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str


async def worker(pool: asyncpg.Pool):
    """Polls DB; processes rows older than 30s. DB is the queue, so restarts lose nothing."""
    while True:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch(
                        """
                        SELECT transaction_id FROM transactions
                        WHERE status = 'PROCESSING'
                          AND created_at <= now() - make_interval(secs => $1)
                        ORDER BY created_at
                        LIMIT 50
                        FOR UPDATE SKIP LOCKED
                        """,
                        PROCESS_DELAY_SECONDS,
                    )
                    ids = [r["transaction_id"] for r in rows]
                    if ids:
                        await conn.execute(
                            """
                            UPDATE transactions
                            SET status = 'PROCESSED', processed_at = now()
                            WHERE transaction_id = ANY($1::text[])
                            """,
                            ids,
                        )
        except Exception as e:
            print(f"worker error: {e}")
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # statement_cache_size=0 is required for Neon's pooled (pgbouncer) connection
    app.state.pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0
    )
    task = asyncio.create_task(worker(app.state.pool))
    yield
    task.cancel()
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def health():
    return {
        "status": "HEALTHY",
        "current_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.post("/v1/webhooks/transactions", status_code=202)
async def receive_webhook(payload: TransactionWebhook):
    # ON CONFLICT DO NOTHING = idempotency (duplicates silently ignored)
    await app.state.pool.execute(
        """
        INSERT INTO transactions
          (transaction_id, source_account, destination_account, amount, currency)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (transaction_id) DO NOTHING
        """,
        payload.transaction_id,
        payload.source_account,
        payload.destination_account,
        payload.amount,
        payload.currency,
    )
    return {"status": "accepted"}


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    row = await app.state.pool.fetchrow(
        "SELECT * FROM transactions WHERE transaction_id = $1", transaction_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    r = dict(row)
    r["amount"] = float(r["amount"])
    r["created_at"] = r["created_at"].isoformat()
    r["processed_at"] = r["processed_at"].isoformat() if r["processed_at"] else None
    return [r]  # spec shows an array