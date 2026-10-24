import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
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


app = FastAPI(lifespan=lifespan, title="Transaction Webhook Service")


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


UI_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Webhook Transaction Demo</title>
<style>
  :root { --bg:#0f1115; --card:#181b22; --line:#2a2f3a; --txt:#e8eaf0; --mut:#8b93a7; --acc:#4f8cff; --ok:#2ecc71; --warn:#f5a623; --bad:#ff5c5c; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px/1.5 system-ui, sans-serif; padding:24px; }
  .wrap { max-width:960px; margin:0 auto; display:grid; gap:16px; }
  h1 { font-size:20px; margin:0 0 4px; }
  p.sub { color:var(--mut); margin:0 0 8px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
  @media (max-width:760px) { .grid { grid-template-columns:1fr; } }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; }
  .card h2 { font-size:14px; margin:0 0 12px; color:var(--mut); text-transform:uppercase; letter-spacing:.05em; }
  label { display:block; color:var(--mut); font-size:12px; margin:8px 0 2px; }
  input { width:100%; padding:8px 10px; background:var(--bg); border:1px solid var(--line); border-radius:6px; color:var(--txt); }
  .row { display:flex; gap:8px; }
  button { padding:8px 12px; border:0; border-radius:6px; background:var(--acc); color:#fff; cursor:pointer; font-weight:600; margin-top:12px; }
  button.sec { background:#2a2f3a; }
  .badge { display:inline-block; padding:4px 12px; border-radius:20px; font-weight:700; font-size:13px; }
  .PROCESSING { background:rgba(245,166,35,.15); color:var(--warn); }
  .PROCESSED { background:rgba(46,204,113,.15); color:var(--ok); }
  .NONE { background:#2a2f3a; color:var(--mut); }
  .kv { margin-top:12px; color:var(--mut); font-size:13px; }
  .kv b { color:var(--txt); font-weight:500; }
  #log { max-height:240px; overflow:auto; font-family:ui-monospace, Consolas, monospace; font-size:12px; }
  #log div { padding:3px 0; border-bottom:1px solid var(--line); }
  .ok { color:var(--ok); } .bad { color:var(--bad); }
  a { color:var(--acc); }
</style>
</head>
<body>
<div class="wrap">
  <div>
    <h1>Transaction Webhook Service</h1>
    <p class="sub">Webhook is acknowledged instantly (202), processed in the background after 30s, duplicates are ignored. &middot; <a href="/docs">API docs</a></p>
  </div>

  <div class="grid">
    <div class="card">
      <h2>1. Send webhook</h2>
      <label>transaction_id</label>
      <div class="row"><input id="tid"><button class="sec" onclick="newId()" style="margin-top:0">New ID</button></div>
      <label>source_account</label><input id="src" value="acc_user_789">
      <label>destination_account</label><input id="dst" value="acc_merchant_456">
      <div class="row">
        <div style="flex:1"><label>amount</label><input id="amt" type="number" value="1500"></div>
        <div style="flex:1"><label>currency</label><input id="cur" value="INR"></div>
      </div>
      <div class="row">
        <button onclick="sendOne()">Send webhook</button>
        <button class="sec" onclick="sendDuplicates()">Send 5 duplicates</button>
      </div>
    </div>

    <div class="card">
      <h2>2. Transaction status</h2>
      <span id="badge" class="badge NONE">NO TRANSACTION</span>
      <div class="kv" id="details">Send a webhook to start tracking it.</div>
    </div>
  </div>

  <div class="card">
    <h2>Request log (status code and response time)</h2>
    <div id="log"></div>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
let timer = null;

function newId() { $('tid').value = 'txn_' + Math.random().toString(36).slice(2, 12); }
newId();

function payload() {
  return {
    transaction_id: $('tid').value.trim(),
    source_account: $('src').value,
    destination_account: $('dst').value,
    amount: Number($('amt').value),
    currency: $('cur').value
  };
}

function log(msg, ok) {
  const d = document.createElement('div');
  d.className = ok ? 'ok' : 'bad';
  d.textContent = new Date().toLocaleTimeString() + '  ' + msg;
  $('log').prepend(d);
}

async function postWebhook() {
  const p = payload();
  const t0 = performance.now();
  try {
    const r = await fetch('/v1/webhooks/transactions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(p)
    });
    const ms = Math.round(performance.now() - t0);
    log('POST ' + p.transaction_id + ' -> ' + r.status + ' in ' + ms + ' ms', r.status === 202);
  } catch (e) {
    log('POST failed: ' + e, false);
  }
}

async function sendOne() { await postWebhook(); startPolling(); }

async function sendDuplicates() {
  await Promise.all([1, 2, 3, 4, 5].map(() => postWebhook()));
  log('5 identical webhooks sent: only ONE transaction is created', true);
  startPolling();
}

function startPolling() {
  if (timer) clearInterval(timer);
  poll();
  timer = setInterval(poll, 2000);
}

async function poll() {
  const id = $('tid').value.trim();
  const r = await fetch('/v1/transactions/' + encodeURIComponent(id));
  if (r.status === 404) {
    $('badge').className = 'badge NONE';
    $('badge').textContent = 'NOT FOUND';
    return;
  }
  const t = (await r.json())[0];
  $('badge').className = 'badge ' + t.status;
  $('badge').textContent = t.status;
  $('details').innerHTML =
    '<div>ID: <b>' + t.transaction_id + '</b></div>' +
    '<div>Amount: <b>' + t.amount + ' ' + t.currency + '</b></div>' +
    '<div>Created: <b>' + t.created_at + '</b></div>' +
    '<div>Processed: <b>' + (t.processed_at || 'pending') + '</b></div>';
  if (t.status === 'PROCESSED') { clearInterval(timer); timer = null; }
}
</script>
</body>
</html>
"""


@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def ui():
    return UI_HTML