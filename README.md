# hotel-triage-worker

Self-updating **J-REIT hotel transaction tracker** running on Cloudflare Workers + D1.
It discovers TDnet filings from hotel-owning J-REITs, uses Gemini to triage which
ones concern a hotel acquisition/disposal, extracts structured deal data (price,
keys, GFA, yield, location…), and serves a filterable map + table dashboard.

---

## Pipeline

```
discover  ->  pull latest TDnet filings (Yanoshin API) into the `filings` queue
triage    ->  Gemini decides: is the traded asset a hotel?  (pending_extraction | rejected_not_hotel)
extract   ->  Gemini pulls structured deal fields into `hotel_transactions`
```

Each stage runs independently so no single invocation times out. The Cloudflare
cron (`*/5 * * * *`) rotates through discover / extract / triage by minute.

---

## Repo layout

```
hotel-triage-worker/
├── src/
│   └── index.js            # the Worker (deployed by wrangler)
├── wrangler.jsonc          # deploy config: binding, cron, D1 database id
├── schema.sql              # reference DDL for the `filings` queue table
├── load-queue.js           # one-off local seeder: hotel_filings.json -> D1 (run with Node)
├── hotel_filings.json      # initial seed data (already loaded into D1)
├── .github/workflows/
│   ├── deploy.yml          # auto-deploy on push to main
│   └── schedule.yml        # external cron backup: pings /run every 10 min
├── .gitignore
└── README.md
```

Only **`src/index.js`** and **`wrangler.jsonc`** are required to deploy. The rest
are reference / one-off tooling.

---

## Routes

| Route          | Purpose                                             |
|----------------|-----------------------------------------------------|
| `/`            | HTML dashboard (map, charts, filterable table)      |
| `/stats`       | JSON queue + extraction counters                    |
| `/discover`    | Pull new TDnet filings into the queue               |
| `/triage`      | Run one triage batch                                |
| `/extract`     | Run one extraction batch                            |
| `/run`         | Extract **and** triage (used by the external cron)  |
| `/api`         | Raw `hotel_transactions` as JSON                    |
| `/export.csv`  | Download all transactions as CSV                    |

---

## Local development

```bash
npm install -g wrangler
wrangler dev            # local server on http://127.0.0.1:8787
```

Seed the queue once (needs a token + DB id in your shell):

```bash
CF_ACCOUNT_ID=... CF_API_TOKEN=... CF_DATABASE_ID=c1c3e23f-8871-49ee-9309-e9cde7295b9f \
  node load-queue.js hotel_filings.json
```

---

## Secrets (Wrangler, **not** git)

```bash
wrangler secret put GEMINI_API_KEYS      # comma-separated keys for rotation
wrangler secret put GDRIVE_CLIENT_ID     # optional: PDF archiving to Drive
wrangler secret put GDRIVE_CLIENT_SECRET
wrangler secret put GDRIVE_REFRESH_TOKEN
wrangler secret put GDRIVE_FOLDER_ID
```

Drive archiving is optional — it only runs if all four `GDRIVE_*` secrets are set.

---

## CI/CD — GitHub Actions

### 1. Add repo secrets
`Settings → Secrets and variables → Actions`:

| Secret                  | Where to get it                                                        |
|-------------------------|------------------------------------------------------------------------|
| `CLOUDFLARE_API_TOKEN`  | Cloudflare dashboard → *Account API Tokens* → template **"Edit Cloudflare Workers"** |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare dashboard → account overview (or `wrangler whoami`)         |

> Scope the token to just this account. Do **not** commit it.

### 2. `.github/workflows/deploy.yml` — auto-deploy on push

```yaml
name: Deploy Worker

on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - name: Build & Deploy Worker
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          # runs `wrangler deploy` using wrangler.jsonc by default
```

### 3. `.github/workflows/schedule.yml` — reliable external cron

A backup trigger in case the Cloudflare cron is flaky. Pings `/run` every 10 min.

```yaml
name: Trigger pipeline

on:
  schedule:
    - cron: "*/10 * * * *"   # UTC, best-effort
  workflow_dispatch:

jobs:
  ping:
    runs-on: ubuntu-latest
    steps:
      - name: Run extraction + triage
        run: |
          curl -sS --fail-with-body \
            "https://hotel-triage-worker.dispatch-news.workers.dev/run" || true
```

Every push to `main` redeploys the Worker; the schedule workflow keeps the queue
draining even if the in-Worker cron misfires.

---

## Deploy manually (Termux / any shell)

```bash
wrangler deploy
time curl -s https://hotel-triage-worker.dispatch-news.workers.dev/extract; echo
curl -s https://hotel-triage-worker.dispatch-news.workers.dev/stats; echo
```

A healthy `/extract` now takes several seconds (Gemini + D1) and
`pending_extraction` should drop on the next `/stats`.

---

## Queue statuses

| Status               | Meaning                                             |
|----------------------|-----------------------------------------------------|
| `pending_triage`     | Awaiting Gemini hotel/not-hotel classification      |
| `rejected_not_hotel` | Triaged as not a hotel deal — stop                  |
| `pending_extraction` | Confirmed hotel, awaiting field extraction          |
| `extracted`          | Full deal data pulled into `hotel_transactions`     |
| `triage_failed`      | Failed triage after max attempts                    |
| `extraction_failed`  | Failed extraction after max attempts (poison item)  |

Extraction/triage retry a flaky item a few times (tracked via `extract_attempts`
/ `triage_attempts`), then mark it `*_failed` so one bad PDF can't jam the queue.
A genuine quota error (HTTP 429) halts the batch cleanly and resumes next run.
