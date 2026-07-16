// index.js (Cloudflare Worker) — self-updating J-REIT hotel transaction tracker.
//
// Stages: discover -> triage -> extract. Each runs independently.
// Routes: /stats, /errors, / (table), /api, /export.csv, /discover, /triage, /extract, /run
//
// PATCH LOG
//  2026-07 (a): queue no longer stalls on one bad item — quota(429) halts the
//               batch & resumes later; transient(503/parse) retries per-item a
//               few times then marks *_failed so the queue drains.
//  2026-07 (b): /extract,/triage,/run now AWAIT the work (fire-and-forget
//               waitUntil was being cancelled after the fetch response).
//  2026-07 (c): fetch() timeouts (PDF 20s, Gemini 45s) so one slow request
//               can't hang the whole awaited batch; EXTRACT_BATCH=1 so an
//               awaited /extract always finishes fast; last_error recorded +
//               /errors route to read failures from anywhere.
//  2026-07 (d): last_error now stage-tagged ([PDF fetch]/[Gemini call]/...);
//               Gemini network-level failures (timeouts) retried in-call like
//               a 503 instead of failing on the first try; *_failed rows are
//               auto-requeued every ~15min (the discover tick) up to a
//               lifetime attempt cap instead of sitting stuck forever;
//               repeated "too large" PDF failures escalate to a stronger,
//               still-free-tier model with a bigger (riskier) byte cap.

const TRIAGE_BATCH = 6;
const EXTRACT_BATCH = 1;     // await'd in fetch handler -> 1 at a time is bulletproof
const DELAY_MS = 300;
const DISCOVER_DAYS = 10;
const FRESH_WINDOW_DAYS = 31;
const MODEL = "gemini-3.1-flash-lite";
// Used only after repeated "PDF too large" failures on the same filing.
// gemini-3.5-flash is Google's current non-lite Flash model and is still
// free-tier eligible as of 2026-07 — but it shares its OWN free-tier RPD
// bucket, separate from and much smaller than Flash-Lite's, so this is meant
// as a rare escalation path, not a routine one. Check your live limits at
// https://aistudio.google.com/rate-limit before leaning on it heavily.
const MODEL_FALLBACK = "gemini-3.5-flash";

const MAX_EXTRACT_ATTEMPTS = 4;    // retries within one burst before flipping to *_failed
const MAX_TRIAGE_ATTEMPTS = 4;
// Lifetime ceiling across ALL requeue cycles (see requeueFailedJobs). Once a
// row's attempts counter hits this, it stays *_failed permanently — check /errors.
const MAX_EXTRACT_LIFETIME_ATTEMPTS = 10;
const MAX_TRIAGE_LIFETIME_ATTEMPTS = 10;
const PDF_TIMEOUT_MS = 20000;
const GEMINI_TIMEOUT_MS = 45000;         // triage — small 3-field schema, should be fast
const GEMINI_TIMEOUT_MS_EXTRACT = 60000; // extraction — heavier 14-field schema, give it more room.
                                          // Wall-clock wait doesn't cost Workers CPU time, so
                                          // this is basically free; it only affects how long a
                                          // manual /extract hit or a cron tick takes to finish.

// Both Flash and Flash-Lite share the same 1M-token context — switching
// models does NOT by itself let a bigger PDF through. This cap is a Workers
// CPU-safety limit on the base64-encoding loop, not a Gemini limit, so it has
// to be raised explicitly for an escalated retry to actually do anything.
const MAX_PDF_BYTES = 8 * 1024 * 1024;            // normal cap
const MAX_PDF_BYTES_ESCALATED = 16 * 1024 * 1024; // used once escalated — raises CPU-limit-kill risk, watch it
const TOO_LARGE_ESCALATE_AFTER = 1; // escalate once a "too large" error has happened this many times
// Safety net: if a filing keeps timing out against flash-lite for ANY reason
// (not just "too large" — e.g. flash-lite having a slow/degraded patch),
// give it a few tries first, then let a completely different model pool have
// a shot instead of retrying the same lane forever.
const GEMINI_TIMEOUT_ESCALATE_AFTER = 3;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const REIT_CODES = new Set([
  "8951","8952","8953","8954","8955","8956","8957","8958","8960","8961","8963",
  "8964","8966","8967","8968","8972","8975","8976","8977","8979","8984","8985",
  "8986","8987","3226","3234","3249","3269","3279","3281","3282","3283","3287",
  "3290","3292","3295","3296","3309","3451","3455","3459","3462","3463","3466",
  "3468","3470","3471","3472","3476","3481","3487","3488","3492","2971","2972",
  "2979","2989","401A",
]);

const PRIORITY_ORDER = `
  ORDER BY (julianday('now') - julianday(pubdate) <= ${FRESH_WINDOW_DAYS}) DESC,
           CASE WHEN julianday('now') - julianday(pubdate) <= ${FRESH_WINDOW_DAYS}
                THEN julianday(pubdate) ELSE -julianday(pubdate) END ASC`;

function getDB(env) {
  const preferred = ["DB", "hotel_filings_db", "hotel_filings", "database"];
  for (const name of preferred) {
    if (env[name] && typeof env[name].prepare === "function") return env[name];
  }
  for (const key of Object.keys(env)) {
    if (env[key] && typeof env[key].prepare === "function") return env[key];
  }
  throw new Error("No D1 binding found. env keys: " + Object.keys(env).join(", "));
}

function getKeys(env) {
  const raw = env.GEMINI_API_KEYS || env.GEMINI_API_KEY || "";
  return raw.split(",").map((k) => k.trim()).filter(Boolean);
}

export default {
  async scheduled(event, env, ctx) {
    await ensureTable(env);
    await ensureColumns(env);
    const min = new Date().getMinutes();
    if (min % 15 === 0)      ctx.waitUntil(requeueFailedJobs(env).then(() => discoverNewFilings(env)));
    else if (min % 2 === 0)  ctx.waitUntil(processExtractionQueue(env));
    else                     ctx.waitUntil(processTriageQueue(env));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    await ensureTable(env).catch(() => {});
    await ensureColumns(env).catch(() => {});

    // AWAIT the work (fire-and-forget waitUntil gets cancelled after response).
    if (url.pathname === "/extract") { await processExtractionQueue(env); return new Response("Extraction batch done.\n"); }
    if (url.pathname === "/triage")  { await processTriageQueue(env);     return new Response("Triage batch done.\n"); }
    if (url.pathname === "/run") {
      await processExtractionQueue(env);
      await processTriageQueue(env);
      return new Response("Extract + triage done.\n");
    }
    if (url.pathname === "/discover") { const n = await discoverNewFilings(env); return new Response(`Discovery: inserted ${n}.\n`); }
    if (url.pathname === "/requeue")  { const n = await requeueFailedJobs(env); return new Response(`Requeued ${n}.\n`); }
    if (url.pathname === "/stats")    return statsResponse(env);

    if (url.pathname === "/errors") {
      const { results } = await getDB(env).prepare(
        `SELECT id, reit_name, title, status, extract_attempts, triage_attempts, last_error
           FROM filings
          WHERE status IN ('extraction_failed','triage_failed')
          ORDER BY id DESC LIMIT 20`
      ).all().catch((e) => ({ results: [{ query_error: String(e.message || e) }] }));
      return Response.json(results);
    }

    const { results } = await getDB(env).prepare(
      `SELECT * FROM hotel_transactions ORDER BY pubdate DESC`
    ).all().catch(() => ({ results: [] }));

    if (url.pathname === "/export.csv") return csvResponse(results);
    if (url.pathname === "/api")        return Response.json(results);
    return htmlResponse(results);
  },
};

// ============================================================================
async function ensureTable(env) {
  await getDB(env).prepare(
    `CREATE TABLE IF NOT EXISTS hotel_transactions (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       filing_id INTEGER UNIQUE,
       reit_name TEXT, pdf_url TEXT, property_name TEXT, property_name_en TEXT, transaction_type TEXT,
       num_rooms INTEGER, gfa_sqm REAL, stake_pct REAL, price_jpy INTEGER,
       location TEXT, latitude REAL, longitude REAL, yield_pct REAL,
       appraisal_jpy INTEGER, closing_date TEXT,
       multi_property INTEGER, needs_review INTEGER, review_note TEXT,
       pubdate TEXT, source TEXT, archive_url TEXT, extracted_at TEXT DEFAULT (datetime('now'))
     )`
  ).run();
}

// Idempotent column adds (SQLite/D1 has no ADD COLUMN IF NOT EXISTS).
async function ensureColumns(env) {
  const db = getDB(env);
  for (const col of ["extract_attempts", "triage_attempts"]) {
    await db.prepare(`ALTER TABLE filings ADD COLUMN ${col} INTEGER DEFAULT 0`).run().catch(() => {});
  }
  await db.prepare(`ALTER TABLE filings ADD COLUMN last_error TEXT`).run().catch(() => {});
}

async function discoverNewFilings(env) {
  const fmt = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
  const end = new Date();
  const start = new Date(Date.now() - DISCOVER_DAYS * 864e5);
  const api = `https://webapi.yanoshin.jp/webapi/tdnet/list/${fmt(start)}-${fmt(end)}.json?limit=300`;

  let items = [];
  try {
    const res = await fetch(api, { signal: AbortSignal.timeout(PDF_TIMEOUT_MS) });
    if (!res.ok) { console.warn(`Discovery: HTTP ${res.status}`); return 0; }
    items = (await res.json()).items || [];
  } catch (e) { console.warn(`Discovery failed: ${e.message}`); return 0; }

  const ACQ_DISP = /取得|譲渡|acquisition|acquire|disposal|disposition|transfer|sale/i;
  let inserted = 0;
  for (const it of items) {
    const t = it.Tdnet || it.tdnet || it;
    const code4 = (t.company_code || t.code || "").toString().slice(0, 4);
    const title = t.title || "";
    const pdf = t.document_url || t.pdf_url || "";
    const pub = (t.pubdate || t.date || "").replace("T", " ");
    if (!code4 || !pdf || !REIT_CODES.has(code4) || !ACQ_DISP.test(title)) continue;
    try {
      const r = await getDB(env).prepare(
        `INSERT INTO filings (pdf_url, reit_code, reit_name, pubdate, title, status)
         VALUES (?, ?, ?, ?, ?, 'pending_triage') ON CONFLICT(pdf_url) DO NOTHING`
      ).bind(pdf, code4, t.company_name || t.company || "", pub, title).run();
      if ((r.meta?.changes ?? 0) > 0) inserted++;
    } catch (e) { console.error(`Discovery insert: ${e.message}`); }
  }
  console.log(`Discovery: scanned ${items.length}, inserted ${inserted}.`);
  return inserted;
}

// Gives *_failed rows another shot instead of leaving them stuck until
// someone runs a manual UPDATE. Attempts counters are NOT reset on requeue —
// they keep accumulating, which is what bounds this to MAX_*_LIFETIME_ATTEMPTS
// and is also what pickGeminiOpts() reads to decide whether to escalate.
async function requeueFailedJobs(env) {
  const db = getDB(env);
  const ext = await db.prepare(
    `UPDATE filings SET status='pending_extraction'
      WHERE status='extraction_failed' AND extract_attempts < ?`
  ).bind(MAX_EXTRACT_LIFETIME_ATTEMPTS).run();
  const tri = await db.prepare(
    `UPDATE filings SET status='pending_triage'
      WHERE status='triage_failed' AND triage_attempts < ?`
  ).bind(MAX_TRIAGE_LIFETIME_ATTEMPTS).run();
  const n = (ext.meta?.changes ?? 0) + (tri.meta?.changes ?? 0);
  if (n) console.log(`Requeue: ${ext.meta?.changes ?? 0} extraction_failed, ${tri.meta?.changes ?? 0} triage_failed -> pending.`);
  return n;
}

async function processTriageQueue(env) {
  const { results: pending } = await getDB(env).prepare(
    `SELECT id, pdf_url, title, pubdate, triage_attempts, last_error FROM filings
      WHERE status='pending_triage' ${PRIORITY_ORDER} LIMIT ?`
  ).bind(TRIAGE_BATCH).all();

  console.log(`Triage: ${pending.length} pending.`);
  for (const row of pending) {
    try {
      const { data: r } = await classifyFiling(row.pdf_url, row.title, env, row.pubdate, row.triage_attempts, row.last_error);
      const st = r.is_hotel ? "pending_extraction" : "rejected_not_hotel";
      await getDB(env).prepare(
        `UPDATE filings SET status=?, triage_is_hotel=?, triage_confidence=?,
                triage_reasoning=?, triaged_at=datetime('now') WHERE id=?`
      ).bind(st, r.is_hotel ? 1 : 0, r.confidence, r.reasoning, row.id).run();
      console.log(`  #${row.id} -> ${st} (${r.confidence})`);
    } catch (err) {
      if (isQuota(err)) { console.warn(`[quota] triage halted at #${row.id}; resume next run.`); break; }
      const attempts = (row.triage_attempts || 0) + 1;
      const msg = String(err && err.message || err).slice(0, 500);
      await getDB(env).prepare(`UPDATE filings SET triage_attempts=?, last_error=? WHERE id=?`).bind(attempts, msg, row.id).run();
      if (isTransient(err) && attempts < MAX_TRIAGE_ATTEMPTS) {
        console.warn(`[retry-later] triage #${row.id} attempt ${attempts}: ${msg}`);
        continue;
      }
      console.error(`Triage failed #${row.id} after ${attempts}: ${msg}`);
      await getDB(env).prepare(`UPDATE filings SET status='triage_failed' WHERE id=?`).bind(row.id).run();
    }
    await sleep(DELAY_MS);
  }
}

async function processExtractionQueue(env) {
  const { results: pending } = await getDB(env).prepare(
    `SELECT id, pdf_url, title, reit_name, pubdate, extract_attempts, last_error FROM filings
      WHERE status='pending_extraction' ${PRIORITY_ORDER} LIMIT ?`
  ).bind(EXTRACT_BATCH).all();

  console.log(`Extraction: ${pending.length} pending.`);
  for (const row of pending) {
    try {
      const { data, pdfAvailable, base64 } = await extractFiling(row.pdf_url, row.title, env, row.pubdate, row.extract_attempts, row.last_error);

      let archiveUrl = null;
      if (base64 && driveConfigured(env)) {
        const name = `${row.pubdate.slice(0,10)}_${row.reit_name}_${(data.property_name_en||data.property_name||row.id)}`
          .replace(/[\\/:*?"<>|]/g, "_").slice(0, 120) + ".pdf";
        archiveUrl = await archiveToDrive(env, name, base64).catch((e) => {
          console.warn(`Drive archive failed #${row.id}: ${e.message}`); return null;
        });
        if (archiveUrl) console.log(`  #${row.id} archived to Drive`);
      }

      await getDB(env).prepare(
        `INSERT INTO hotel_transactions
           (filing_id, reit_name, pdf_url, property_name, property_name_en, transaction_type,
            num_rooms, gfa_sqm, stake_pct, price_jpy, location, latitude, longitude, yield_pct,
            appraisal_jpy, closing_date, multi_property, needs_review, review_note,
            pubdate, source, archive_url)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(filing_id) DO NOTHING`
      ).bind(
        row.id, row.reit_name, row.pdf_url, data.property_name, data.property_name_en ?? null,
        data.transaction_type, data.num_rooms ?? null, data.gfa_sqm ?? null, data.stake_pct ?? null,
        data.price_jpy ?? null, data.location ?? null, data.latitude ?? null, data.longitude ?? null,
        data.yield_pct ?? null, data.appraisal_jpy ?? null, data.closing_date ?? null,
        data.multi_property ? 1 : 0, data.needs_review ? 1 : 0, data.review_note ?? null,
        row.pubdate, pdfAvailable ? "pdf" : "title_only", archiveUrl
      ).run();

      await getDB(env).prepare(`UPDATE filings SET status='extracted' WHERE id=?`).bind(row.id).run();
      const flag = data.needs_review ? " ⚠needs_review" : "";
      console.log(`  #${row.id} extracted: ${data.property_name} (${data.transaction_type})${flag}`);
    } catch (err) {
      if (isQuota(err)) { console.warn(`[quota] extract halted at #${row.id}; resume next run.`); break; }
      const attempts = (row.extract_attempts || 0) + 1;
      const msg = String(err && err.message || err).slice(0, 500);
      await getDB(env).prepare(`UPDATE filings SET extract_attempts=?, last_error=? WHERE id=?`).bind(attempts, msg, row.id).run();
      if (isTransient(err) && attempts < MAX_EXTRACT_ATTEMPTS) {
        console.warn(`[retry-later] extract #${row.id} attempt ${attempts}: ${msg}`);
        continue;
      }
      console.error(`Extraction failed #${row.id} after ${attempts}: ${msg}`);
      await getDB(env).prepare(`UPDATE filings SET status='extraction_failed' WHERE id=?`).bind(row.id).run();
    }
    await sleep(DELAY_MS);
  }
}

// ============================================================================
// GEMINI CALLS
// ============================================================================
// Decides which model/byte-cap to use for THIS attempt, based on what the
// last attempt's error was. Only "too large" escalates — timeouts/5xx/etc.
// already retry on the same model via isTransient(), and switching models
// wouldn't help those anyway.
function pickGeminiOpts(lastError, attempts, geminiTimeoutMs) {
  const tooLarge = /too large/i.test(lastError || "");
  const repeatedTimeout = /\[Gemini call\][^(]*timeout/i.test(lastError || "");
  const escalate =
    (tooLarge && (attempts || 0) >= TOO_LARGE_ESCALATE_AFTER) ||
    (repeatedTimeout && (attempts || 0) >= GEMINI_TIMEOUT_ESCALATE_AFTER);
  const base = escalate
    ? { model: MODEL_FALLBACK, maxPdfBytes: MAX_PDF_BYTES_ESCALATED }
    : { model: MODEL, maxPdfBytes: MAX_PDF_BYTES };
  return { ...base, geminiTimeoutMs };
}

async function classifyFiling(pdfUrl, title, env, pubdate, attempts, lastError) {
  const prompt =
    "This is a Japanese J-REIT disclosure filing (TDnet). Decide whether the ASSET " +
    "BEING ACQUIRED OR DISPOSED is itself a HOTEL / resort / lodging property.\n" +
    "IMPORTANT distinctions:\n" +
    "- Answer TRUE only if the traded asset's primary use is a hotel/lodging, OR a " +
    "hotel is a material part of what is actually being bought/sold.\n" +
    "- Answer FALSE if the filing merely MENTIONS a hotel incidentally — e.g. the REIT " +
    "is buying only the office/retail portion (店舗・事務所部分) of a mixed-use building " +
    "that happens to contain a hotel, or the hotel is just a neighbouring/adjacent site.\n" +
    "- Answer FALSE for offices, logistics, residential, retail, land-only (底地), " +
    "credit ratings, unit buybacks, or trademark deals.\n\nFiling title: " + title;

  const schema = {
    type: "object",
    properties: {
      is_hotel:   { type: "boolean" },
      confidence: { type: "string", enum: ["high", "medium", "low"] },
      reasoning:  { type: "string" },
    },
    required: ["is_hotel", "confidence", "reasoning"],
  };
  return callGemini(prompt, pdfUrl, schema, env, pubdate, pickGeminiOpts(lastError, attempts));
}

async function extractFiling(pdfUrl, title, env, pubdate, attempts, lastError) {
  const prompt =
    "This is a Japanese J-REIT hotel transaction disclosure (TDnet). Extract the deal " +
    "for the HOTEL asset being acquired/disposed. Follow these rules carefully:\n" +
    "- price_jpy, appraisal_jpy: full yen (not millions; 百万円×1,000,000).\n" +
    "- num_rooms: guest room / key count (客室数).\n" +
    "- gfa_sqm: gross floor area / 延床面積 (sqm).\n" +
    "- stake_pct: if the REIT is buying only a co-ownership / quasi-co-ownership share " +
    "(準共有持分 / 共有持分 / 区分所有), report that percentage (e.g. 49.0). If it's a " +
    "100% / whole-asset deal, use 100.\n" +
    "- yield_pct: reported NOI / appraisal cap rate (還元利回り) as a percent number.\n" +
    "- location: city + district, e.g. \"Tokyo, Chuo\".\n" +
    "- latitude, longitude: approximate decimal coordinates of the property from its " +
    "address (e.g. Tokyo Chuo ~35.67, 139.77). Give your best estimate from the stated address.\n" +
    "- closing_date: settlement/transfer date YYYY-MM-DD.\n" +
    "- multi_property: true if the filing covers MORE THAN ONE property (交換 / 複数物件 / " +
    "a portfolio). Report the primary hotel in the other fields.\n" +
    "- needs_review: set TRUE if ANY of these apply: multi_property is true; stake_pct < 100 " +
    "(partial stake, so gfa/price are not directly comparable); the traded asset is only part " +
    "of a mixed-use building; or the price/appraisal is withheld (非開示).\n" +
    "- review_note: one short sentence explaining why review is needed (or why data is sparse).\n" +
    "- property_name: the property name as written in the filing (keep Japanese if that is how it appears).\n" +
    "- property_name_en: an English translation / romanisation of the property name.\n\n" +
    "IMPORTANT: The property-detail table (物件の内容 / 本取得予定資産の内容) and the appraisal " +
    "section (鑑定評価書の概要) contain 延床面積, 取得価格, 鑑定評価額, 還元利回り, 所在地, 取得日 etc. " +
    "Extract EVERY one of these that appears — do not leave a field blank if its value is in the document. " +
    "Only omit a field if it is genuinely absent or marked 非開示.\n\nFiling title: " + title;

  const schema = {
    type: "object",
    properties: {
      property_name:    { type: "string" },
      property_name_en: { type: "string" },
      transaction_type: { type: "string", enum: ["acquisition", "disposal"] },
      num_rooms:        { type: "integer" },
      gfa_sqm:          { type: "number" },
      stake_pct:        { type: "number" },
      price_jpy:        { type: "number" },
      location:         { type: "string" },
      latitude:         { type: "number" },
      longitude:        { type: "number" },
      yield_pct:        { type: "number" },
      appraisal_jpy:    { type: "number" },
      closing_date:     { type: "string" },
      multi_property:   { type: "boolean" },
      needs_review:     { type: "boolean" },
      review_note:      { type: "string" },
    },
    required: ["property_name", "transaction_type"],
  };

  // When there's no PDF, the prompt already tells the model to leave numeric/
  // geo/date fields empty — so don't also force it to reason over all 16
  // fields of the full schema. A much lighter ask, matching the size of
  // triage's schema (which never has this problem).
  const titleOnlySchema = {
    type: "object",
    properties: {
      property_name:    { type: "string" },
      property_name_en: { type: "string" },
      transaction_type: { type: "string", enum: ["acquisition", "disposal"] },
      multi_property:   { type: "boolean" },
      needs_review:     { type: "boolean" },
      review_note:      { type: "string" },
    },
    required: ["property_name", "transaction_type"],
  };

  const opts = { ...pickGeminiOpts(lastError, attempts, GEMINI_TIMEOUT_MS_EXTRACT), titleOnlySchema };
  return callGemini(prompt, pdfUrl, schema, env, pubdate, opts);
}

// Tags an error with WHICH stage produced it, so last_error reads e.g.
// "[Gemini call] timeout after 45s" instead of a bare "aborted due to
// timeout" that could just as easily have come from the PDF fetch.
// Keeps the word "timeout" in the message so isTransient() still matches it.
function stageError(stage, e, timeoutMs) {
  const isAbort = e?.name === "AbortError" || e?.name === "TimeoutError" ||
    /abort|timeout/i.test(String(e?.message ?? e));
  const detail = isAbort ? `timeout after ${(timeoutMs / 1000).toFixed(0)}s` : String(e?.message ?? e);
  return new Error(`[${stage}] ${detail}`);
}

// If the PDF fetch already failed before we got to Gemini, fold that context
// into the final thrown error so a Gemini-stage failure doesn't hide the
// fact that the PDF stage failed first (both can be visible in last_error).
function withPdfNote(err, pdfNote) {
  if (pdfNote) err.message = `${err.message} (also: ${pdfNote})`;
  return err;
}

async function callGemini(promptText, pdfUrl, schema, env, pubdate, opts = {}) {
  const keys = getKeys(env);
  if (keys.length === 0) throw new Error("No Gemini API key configured.");
  const model = opts.model || MODEL;
  const maxPdfBytes = opts.maxPdfBytes || MAX_PDF_BYTES;
  const geminiTimeoutMs = opts.geminiTimeoutMs || GEMINI_TIMEOUT_MS;
  if (model !== MODEL) console.log(`  [escalated] using ${model}, maxPdfBytes=${(maxPdfBytes / 1048576).toFixed(0)}MB`);

  let pdfNote = null;
  const fetched = await fetchPdfAsBase64(pdfUrl, pubdate, maxPdfBytes).catch((e) => { pdfNote = e.message; console.warn(e.message); return null; });
  const base64 = fetched?.base64 || null;

  const parts = [{ text: promptText }];
  if (base64) parts.push({ inlineData: { mimeType: "application/pdf", data: base64 } });
  else parts[0].text += "\n\n(NOTE: PDF unavailable — judge from title; leave numeric fields empty and set needs_review true.)";

  const effectiveSchema = (!base64 && opts.titleOnlySchema) ? opts.titleOnlySchema : schema;

  const payload = {
    contents: [{ parts }],
    generationConfig: { responseMimeType: "application/json", responseSchema: effectiveSchema, temperature: 0 },
  };

  let res, errorText;
  const maxAttempts = Math.max(4, keys.length);
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const apiKey = keys[attempt % keys.length];
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`;
    try {
      res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(geminiTimeoutMs),
      });
    } catch (e) {
      // A network-level failure (incl. timeout) here means the request never
      // got a response at all. Earlier this looped through every key on a
      // timeout too, same as a 503 — but with multiple keys configured, that
      // meant ONE stuck filing burned a wasted, timed-out request against
      // EVERY key's quota. If it's genuinely one flaky key, a single retry on
      // the next key still catches that. If it's not (all keys affected at
      // once — account throttling, or Gemini/network having a bad moment),
      // more attempts right now won't help; the next cron tick (2 min later)
      // or the requeue sweep (15 min later) gives real recovery time instead.
      if (attempt >= 1 || attempt === maxAttempts - 1) throw withPdfNote(stageError("Gemini call", e, geminiTimeoutMs), pdfNote);
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }
    if (res.ok) break;
    errorText = await res.text();
    const retryable = [429, 500, 503].includes(res.status);
    if (!retryable || attempt === maxAttempts - 1)
      throw withPdfNote({ status: res.status, message: `[Gemini call] HTTP ${res.status}: ${errorText}` }, pdfNote);
    await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, Math.min(attempt, 3))));
  }

  const json = await res.json();
  const text = json.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!text) throw withPdfNote(new Error("[Gemini response] no text returned"), pdfNote);
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw withPdfNote(new Error(`[Gemini response] invalid JSON: ${e.message}`), pdfNote);
  }
  return { data, pdfAvailable: !!base64, base64, sourceUrl: fetched?.sourceUrl || null };
}

// ============================================================================
async function fetchPdfAsBase64(pdfUrl, pubdate, maxBytes = MAX_PDF_BYTES) {
  let res;
  try {
    res = await fetch(pdfUrl, { redirect: "follow", signal: AbortSignal.timeout(PDF_TIMEOUT_MS) });
  } catch (e) { throw stageError("PDF fetch", e, PDF_TIMEOUT_MS); }
  if (!res.ok) throw new Error(`[PDF fetch] HTTP ${res.status}`);
  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (!ct.includes("application/pdf")) {
    const html = await res.text();
    const m = html.match(/https?:\/\/[^"'\s)]+\.pdf/i);
    if (!m) throw new Error("[PDF fetch] no PDF at URL (source expired)");
    let p;
    try {
      p = await fetch(m[0], { redirect: "follow", signal: AbortSignal.timeout(PDF_TIMEOUT_MS) });
    } catch (e) { throw stageError("PDF fetch", e, PDF_TIMEOUT_MS); }
    if (!p.ok || !(p.headers.get("content-type") || "").toLowerCase().includes("pdf"))
      throw new Error("[PDF fetch] linked PDF unavailable");
    return { base64: await toBase64(p, maxBytes), sourceUrl: m[0] };
  }
  return { base64: await toBase64(res, maxBytes), sourceUrl: pdfUrl };
}

async function toBase64(res, maxBytes = MAX_PDF_BYTES) {
  const buf = new Uint8Array(await res.arrayBuffer());
  // Reject oversized PDFs BEFORE the CPU-heavy loop, so a huge file fails fast
  // (catchable -> recorded in last_error, and title-only fallback still works)
  // instead of blowing the isolate's CPU budget and dying with no error.
  if (buf.length > maxBytes) {
    throw new Error(`[PDF fetch] too large: ${(buf.length / 1048576).toFixed(1)}MB > ${maxBytes / 1048576}MB`);
  }
  let binary = "", chunk = 0x8000;
  for (let i = 0; i < buf.length; i += chunk) binary += String.fromCharCode.apply(null, buf.subarray(i, i + chunk));
  return btoa(binary);
}


// ---- Google Drive archiving (optional) -------------------------------------
function driveConfigured(env) {
  return !!(env.GDRIVE_CLIENT_ID && env.GDRIVE_CLIENT_SECRET &&
            env.GDRIVE_REFRESH_TOKEN && env.GDRIVE_FOLDER_ID);
}
let _driveToken = { value: null, exp: 0 };
async function driveAccessToken(env) {
  if (_driveToken.value && Date.now() < _driveToken.exp) return _driveToken.value;
  const body = new URLSearchParams({
    client_id: env.GDRIVE_CLIENT_ID,
    client_secret: env.GDRIVE_CLIENT_SECRET,
    refresh_token: env.GDRIVE_REFRESH_TOKEN,
    grant_type: "refresh_token",
  });
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body,
    signal: AbortSignal.timeout(PDF_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`Drive token HTTP ${res.status}: ${await res.text()}`);
  const j = await res.json();
  _driveToken = { value: j.access_token, exp: Date.now() + (j.expires_in - 60) * 1000 };
  return _driveToken.value;
}
async function archiveToDrive(env, filename, base64) {
  const token = await driveAccessToken(env);
  const meta = { name: filename, parents: [env.GDRIVE_FOLDER_ID] };
  const boundary = "----drive" + Math.random().toString(36).slice(2);
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const enc = new TextEncoder();
  const pre = enc.encode(
    `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n` +
    JSON.stringify(meta) + `\r\n--${boundary}\r\nContent-Type: application/pdf\r\n\r\n`
  );
  const post = enc.encode(`\r\n--${boundary}--`);
  const bodyBuf = new Uint8Array(pre.length + bytes.length + post.length);
  bodyBuf.set(pre, 0); bodyBuf.set(bytes, pre.length); bodyBuf.set(post, pre.length + bytes.length);
  const res = await fetch(
    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink",
    { method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": `multipart/related; boundary=${boundary}` },
      body: bodyBuf, signal: AbortSignal.timeout(GEMINI_TIMEOUT_MS) }
  );
  if (!res.ok) throw new Error(`Drive upload HTTP ${res.status}: ${await res.text()}`);
  const j = await res.json();
  return j.webViewLink || (j.id ? `https://drive.google.com/file/d/${j.id}/view` : null);
}

// ---- Error classification --------------------------------------------------
function isQuota(err) {
  const code = err?.status ?? err?.statusCode;
  const s = String(err?.message ?? err ?? "");
  return code === 429 || /\b429\b|RESOURCE_EXHAUSTED|Too Many Requests|quota/i.test(s);
}
function isTransient(err) {
  const code = err?.status ?? err?.statusCode;
  const s = String(err?.message ?? err ?? "");
  // include AbortError (timeout) as transient so a slow item retries then fails cleanly
  return code === 500 || code === 503 ||
    /\b50[03]\b|UNAVAILABLE|high demand|overloaded|aborted|timeout|The operation was aborted/i.test(s);
}

// ============================================================================
async function statsResponse(env) {
  const db = getDB(env);
  const safe = async (sql) => (await db.prepare(sql).all().catch(() => ({ results: [] }))).results;
  const status = await safe(`SELECT status, COUNT(*) AS n FROM filings GROUP BY status ORDER BY n DESC`);
  const source = await safe(`SELECT COALESCE(source,'(none)') AS source, COUNT(*) AS n FROM hotel_transactions GROUP BY source`);
  const totals = await safe(`SELECT COUNT(*) AS extracted,
     SUM(CASE WHEN transaction_type='acquisition' THEN 1 ELSE 0 END) AS acquisitions,
     SUM(CASE WHEN transaction_type='disposal' THEN 1 ELSE 0 END) AS disposals,
     SUM(CASE WHEN needs_review=1 THEN 1 ELSE 0 END) AS needs_review,
     SUM(CASE WHEN price_jpy IS NOT NULL THEN 1 ELSE 0 END) AS with_price,
     SUM(CASE WHEN num_rooms IS NOT NULL THEN 1 ELSE 0 END) AS with_rooms
     FROM hotel_transactions`);
  const fresh = await safe(`SELECT COUNT(*) AS fresh_pending FROM filings
     WHERE status='pending_triage' AND julianday('now')-julianday(pubdate) <= ${FRESH_WINDOW_DAYS}`);
  const latest = await safe(`SELECT MAX(pubdate) AS latest_filing FROM filings`);
  return Response.json({
    generated_at: new Date().toISOString(),
    queue_by_status: status,
    fresh_window_pending_triage: fresh[0]?.fresh_pending ?? 0,
    extracted_by_source: source,
    extraction_totals: totals[0] || {},
    latest_filing_in_queue: latest[0]?.latest_filing || null,
  });
}

// ---- Derived metrics + formatters ------------------------------------------
function derive(r) {
  const price = Number(r.price_jpy) || null, appraisal = Number(r.appraisal_jpy) || null;
  const rooms = Number(r.num_rooms) || null, gfa = Number(r.gfa_sqm) || null;
  const stake = Number(r.stake_pct) || null;
  const gfaAdj = (gfa && stake && stake < 100) ? gfa * stake / 100 : gfa;
  return {
    apprPremium: (appraisal && price) ? appraisal / price : null,
    pricePerKey: (price && rooms) ? Math.round(price / rooms) : null,
    pricePerSqm: (price && gfaAdj) ? Math.round(price / gfaAdj) : null,
  };
}

const jpy = (v) => (v || v === 0) ? Number(v).toLocaleString() : "";
const CSV_COLS = ["pubdate","pdf_url","reit_name","property_name","property_name_en","transaction_type","num_rooms",
  "gfa_sqm","stake_pct","price_jpy","location","latitude","longitude","yield_pct","appraisal_jpy","appraisal_premium",
  "price_per_key","price_per_sqm","closing_date","multi_property","needs_review","review_note","source","archive_url"];

function csvResponse(rows) {
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const lines = [CSV_COLS.join(",")];
  for (const r of rows) {
    const d = derive(r);
    const rec = { ...r,
      appraisal_premium: d.apprPremium ? d.apprPremium.toFixed(3) : "",
      price_per_key: d.pricePerKey ?? "", price_per_sqm: d.pricePerSqm ?? "" };
    lines.push(CSV_COLS.map((c) => esc(rec[c])).join(","));
  }
  return new Response(lines.join("\n"), {
    headers: { "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": 'attachment; filename="hotel_transactions.csv"' },
  });
}

function htmlResponse(rows) {
  const data = rows.map((r) => {
    const d = derive(r);
    return {
      pubdate: (r.pubdate || "").slice(0, 10),
      pdf_url: r.pdf_url || "",
      reit_name: r.reit_name || "",
      property_name: r.property_name || "",
      property_name_en: r.property_name_en || "",
      transaction_type: r.transaction_type || "",
      num_rooms: r.num_rooms, gfa_sqm: r.gfa_sqm, stake_pct: r.stake_pct,
      price_jpy: r.price_jpy, location: r.location || "",
      lat: r.latitude, lng: r.longitude,
      yield_pct: r.yield_pct, appraisal_jpy: r.appraisal_jpy,
      appr_premium: d.apprPremium, price_per_key: d.pricePerKey, price_per_sqm: d.pricePerSqm,
      closing_date: r.closing_date || "", needs_review: !!r.needs_review,
      review_note: r.review_note || "", source: r.source || "", archive_url: r.archive_url || "",
    };
  });
  const json = JSON.stringify(data).replace(/</g, "\\u003c");
  const html = `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>J-REIT Hotel Transactions</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body{font-family:system-ui,sans-serif;margin:1rem;color:#111}
  h1{font-size:1.2rem;margin:0 0 .5rem}
  .controls{display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin:.5rem 0 1rem;font-size:13px}
  .controls label{display:flex;flex-direction:column;gap:3px}
  .controls>label>input[type=checkbox], .controls label:has(input[type=checkbox]){flex-direction:row;align-items:center}
  select,input,button{padding:4px 6px;font-size:13px}
  select[multiple]{min-width:130px}
  #fReset{cursor:pointer}
  a.btn{padding:6px 12px;background:#2563eb;color:#fff;text-decoration:none;border-radius:4px}
  #map{height:340px;border:1px solid #ccc;border-radius:6px;margin-bottom:1rem}
  .charts{display:flex;flex-wrap:wrap;gap:16px;margin-bottom:1rem}
  .chartbox{flex:1;min-width:320px;height:300px;border:1px solid #eee;border-radius:6px;padding:8px}
  .chartbox h3{margin:.2rem 0 .4rem;font-size:13px;color:#333;font-weight:600}
  .wrap{overflow-x:auto}
  table{border-collapse:collapse;width:100%;font-size:12px}
  th,td{border:1px solid #ddd;padding:5px 7px;white-space:nowrap;vertical-align:top}
  th{background:#f4f4f4;text-align:left;cursor:pointer}
  td.n{text-align:right}
  td.prop{white-space:normal;min-width:180px}
  td.rev-cell{white-space:normal;min-width:240px;max-width:340px;color:#b45309}
  tr.rev{background:#fff7ed}
  .muted{color:#666;font-size:12px}
</style></head><body>
<h1>J-REIT Hotel Transactions <span id="count" class="muted"></span></h1>
<div class="controls">
  <label>Type<br><select id="fType" multiple size="2">
      <option value="acquisition">Acquisition</option>
      <option value="disposal">Disposal</option></select></label>
  <label>City<br><select id="fCity" multiple size="4"></select></label>
  <label>Ward<br><select id="fWard" multiple size="4"></select></label>
  <div style="display:flex;flex-direction:column;gap:4px">
    <label><input type="checkbox" id="fClean"> Hide needs-review</label>
    <label><input type="checkbox" id="fData"> Only rows with price</label>
    <button type="button" id="fReset">Clear filters</button>
  </div>
  <label>Search<br><input id="fText" placeholder="property / REIT…"></label>
  <a class="btn" href="/export.csv">Download CSV</a>
</div>
<div class="muted" style="margin:-6px 0 10px">Multi-select: Ctrl/Cmd-click (or tap) to pick several; none selected = all.</div>
<div id="map"></div>
<div class="charts">
  <div class="chartbox"><h3>Price / Key (¥m) vs. Yield (%) — filtered rows</h3><canvas id="scatter"></canvas></div>
  <div class="chartbox"><h3>Acquisitions vs. Disposals (¥bn) by quarter — filtered rows</h3><canvas id="bars"></canvas></div>
</div>
<div class="wrap"><table id="tbl"><thead><tr>
  <th>Disclosed</th><th>Link</th><th>REIT</th><th>Property</th><th>Type</th>
  <th>Keys</th><th>GFA (sqm)</th><th>Price (JPY)</th><th>Location</th>
  <th>Yield</th><th>Appraisal (JPY)</th><th>Appr. Premium</th>
  <th>Price / Key</th><th>Price / sqm</th><th>Closing</th><th>Review</th>
</tr></thead><tbody id="tb"></tbody></table></div>
<script>
const DATA = ${json};
const fmt = v => (v||v===0) ? Number(v).toLocaleString() : "";
const num = (v,n=2) => (v==null || v==="") ? "" : (+(+v).toFixed(n)).toString();
function splitLoc(loc){
  if(!loc) return {city:"",ward:""};
  const i = loc.indexOf(",");
  if(i<0) return {city:loc.trim(),ward:""};
  return {city:loc.slice(0,i).trim(), ward:loc.slice(i+1).trim()};
}
DATA.forEach(r=>{ const s=splitLoc(r.location); r._city=s.city; r._ward=s.ward; });
const fType=document.getElementById('fType');
const fCity=document.getElementById('fCity');
const fWard=document.getElementById('fWard');
const selected = sel => [...sel.selectedOptions].map(o=>o.value);
function fillSelect(sel, values){
  const keep=new Set(selected(sel));
  sel.innerHTML='';
  values.forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v;
    if(keep.has(v)) o.selected=true; sel.appendChild(o); });
}
const cities = [...new Set(DATA.map(r=>r._city).filter(Boolean))].sort();
fillSelect(fCity, cities);
function refreshWards(){
  const cs=selected(fCity);
  const wards=[...new Set(DATA
    .filter(r=> cs.length===0 || cs.includes(r._city))
    .map(r=>r._ward).filter(Boolean))].sort();
  fillSelect(fWard, wards);
}
refreshWards();
const map = L.map('map').setView([36.2,138.2], 5);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  {maxZoom:19, attribution:'© OpenStreetMap'}).addTo(map);
let markers = [];
function apply(){
  const types=selected(fType), cs=selected(fCity), ws=selected(fWard);
  const clean=document.getElementById('fClean').checked;
  const dataOnly=document.getElementById('fData').checked;
  const q=document.getElementById('fText').value.toLowerCase();
  const rows = DATA.filter(r=>{
    if(types.length && !types.includes(r.transaction_type)) return false;
    if(cs.length && !cs.includes(r._city)) return false;
    if(ws.length && !ws.includes(r._ward)) return false;
    if(clean && r.needs_review) return false;
    if(dataOnly && !r.price_jpy) return false;
    if(q){ const blob=(r.property_name+" "+r.property_name_en+" "+r.reit_name+" "+r.location).toLowerCase();
           if(!blob.includes(q)) return false; }
    return true;
  });
  render(rows);
}
function render(rows){
  document.getElementById('count').textContent='('+rows.length+' of '+DATA.length+')';
  const tb=document.getElementById('tb'); tb.innerHTML='';
  markers.forEach(m=>map.removeLayer(m)); markers=[];
  const pts=[];
  for(const r of rows){
    const tr=document.createElement('tr'); if(r.needs_review) tr.className='rev';
    const prop=r.property_name + (r.property_name_en?'<br><small style="color:#555">'+r.property_name_en+'</small>':'');
    const stake=(r.stake_pct!=null && r.stake_pct<100)?' <small>('+num(r.stake_pct,1)+'%)</small>':'';
    tr.innerHTML='<td>'+r.pubdate+'</td>'+
      '<td>'+(r.pdf_url?'<a href="'+r.pdf_url+'" target="_blank">view</a>':'')+
        (r.archive_url?' · <a href="'+r.archive_url+'" target="_blank" title="Archived copy in Google Drive">📁</a>':'')+'</td>'+
      '<td>'+r.reit_name+'</td><td class="prop">'+prop+'</td><td>'+r.transaction_type+'</td>'+
      '<td class="n">'+(r.num_rooms??'')+'</td><td class="n">'+(r.gfa_sqm?fmt(r.gfa_sqm):'')+stake+'</td>'+
      '<td class="n">'+fmt(r.price_jpy)+'</td><td>'+r.location+'</td>'+
      '<td class="n">'+(r.yield_pct!=null?num(r.yield_pct,2)+'%':'')+'</td>'+
      '<td class="n">'+fmt(r.appraisal_jpy)+'</td>'+
      '<td class="n">'+(r.appr_premium?r.appr_premium.toFixed(2)+'x':'')+'</td>'+
      '<td class="n">'+fmt(r.price_per_key)+'</td><td class="n">'+fmt(r.price_per_sqm)+'</td>'+
      '<td>'+r.closing_date+'</td>'+
      '<td class="rev-cell">'+(r.needs_review?'⚠ '+r.review_note:'')+'</td>';
    tb.appendChild(tr);
    if(typeof r.lat==='number' && typeof r.lng==='number'){
      const color=r.transaction_type==='disposal'?'#dc2626':'#2563eb';
      const m=L.circleMarker([r.lat,r.lng],{radius:7,color:'#fff',weight:2,fillColor:color,fillOpacity:.9}).addTo(map);
      m.bindPopup('<b>'+(r.property_name_en||r.property_name)+'</b><br>'+r.reit_name+'<br>'+
        r.transaction_type+(r.price_jpy?' · ¥'+fmt(r.price_jpy):'')+'<br>'+r.location);
      markers.push(m); pts.push([r.lat,r.lng]);
    }
  }
  if(pts.length) map.fitBounds(pts,{padding:[30,30],maxZoom:13});
  renderCharts(rows);
}
let scatterChart, barsChart;
function quarterOf(d){
  if(!d || d.length<7) return null;
  const y=d.slice(0,4), m=+d.slice(5,7); return y+" Q"+(Math.floor((m-1)/3)+1);
}
function renderCharts(rows){
  const acq=[], dis=[];
  for(const r of rows){
    if(r.price_per_key==null || r.yield_pct==null) continue;
    const pt={x:+(+r.yield_pct).toFixed(2), y:Math.round(r.price_per_key/1e6*10)/10,
      label:(r.property_name_en||r.property_name)};
    (r.transaction_type==='disposal'?dis:acq).push(pt);
  }
  const sctx=document.getElementById('scatter');
  if(scatterChart) scatterChart.destroy();
  scatterChart=new Chart(sctx,{type:'scatter',
    data:{datasets:[
      {label:'Acquisition',data:acq,backgroundColor:'#2563eb'},
      {label:'Disposal',data:dis,backgroundColor:'#dc2626'}]},
    options:{maintainAspectRatio:false,
      plugins:{tooltip:{callbacks:{label:c=>c.raw.label+': '+c.raw.y+'¥m/key, '+c.raw.x+'%'}}},
      scales:{x:{title:{display:true,text:'Yield (%)'}},
              y:{title:{display:true,text:'Price / Key (¥m)'}}}}});
  const q={};
  for(const r of rows){
    if(!r.price_jpy) continue;
    const key=quarterOf(r.pubdate); if(!key) continue;
    q[key]=q[key]||{a:0,d:0};
    if(r.transaction_type==='disposal') q[key].d+=r.price_jpy; else q[key].a+=r.price_jpy;
  }
  const labels=Object.keys(q).sort();
  const bctx=document.getElementById('bars');
  if(barsChart) barsChart.destroy();
  barsChart=new Chart(bctx,{type:'bar',
    data:{labels,datasets:[
      {label:'Acquisitions',data:labels.map(k=>+(q[k].a/1e9).toFixed(2)),backgroundColor:'#2563eb'},
      {label:'Disposals',data:labels.map(k=>+(q[k].d/1e9).toFixed(2)),backgroundColor:'#dc2626'}]},
    options:{maintainAspectRatio:false,
      scales:{y:{title:{display:true,text:'¥bn'}}}}});
}
fCity.addEventListener('change', ()=>{ refreshWards(); apply(); });
['fType','fWard','fClean','fData','fText'].forEach(id=>{
  document.getElementById(id).addEventListener('input',apply);
});
document.getElementById('fReset').addEventListener('click', ()=>{
  [fType,fCity,fWard].forEach(s=>[...s.options].forEach(o=>o.selected=false));
  document.getElementById('fClean').checked=false;
  document.getElementById('fData').checked=false;
  document.getElementById('fText').value='';
  refreshWards(); apply();
});
apply();
</script></body></html>`;
  return new Response(html, { headers: { "Content-Type": "text/html; charset=utf-8" } });
}
