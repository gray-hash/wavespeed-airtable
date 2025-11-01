/**
 * server.mjs
 *
 * Tiny Render-ready Express app for submitting batch Seedream v4 jobs
 * and archiving results to Airtable.
 *
 * Install:
 *   npm install express node-fetch
 *
 * Required env vars:
 *   AIRTABLE_BASE_ID    e.g. app...
 *   AIRTABLE_TABLE      e.g. tbl...
 *   AIRTABLE_TOKEN      Airtable PAT with read+write to that base
 *   WAVESPEED_API_KEY   WaveSpeed API key for Seedream v4
 *   PUBLIC_BASE_URL     e.g. https://your-app.onrender.com  (used for webhook URLs)
 *
 * Important: adapt WAVESPEED_* endpoints/JSON if your provider's spec differs.
 */

import express from "express";
import fetch from "node-fetch";
import { Buffer } from "buffer";
import crypto from "crypto";

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "20mb" }));

/* ---------- Config ---------- */
const {
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE,
  AIRTABLE_TOKEN,
  WAVESPEED_API_KEY,
  PUBLIC_BASE_URL = "",
  PORT = 3000,
} = process.env;

if (!AIRTABLE_BASE_ID || !AIRTABLE_TABLE || !AIRTABLE_TOKEN || !WAVESPEED_API_KEY) {
  console.error("Missing required env vars. Please set AIRTABLE_BASE_ID, AIRTABLE_TABLE, AIRTABLE_TOKEN, WAVESPEED_API_KEY");
  process.exit(1);
}

const AIRTABLE_API_ROOT = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE}`;
const AIRTABLE_META = `https://api.airtable.com/v0/meta/bases/${AIRTABLE_BASE_ID}/tables`;
const WAVESPEED_SUBMIT_URL = "https://api.wavespeed.ai/v1/seedream/generate"; // <-- adapt if different
const WAVESPEED_POLL_URL = "https://api.wavespeed.ai/v1/requests"; // GET /requests/{id} assumed

/* ---------- Helpers ---------- */

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

async function fetchImageAsDataURL(url) {
  // fetch remote image and return data URL (base64)
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.status}`);
  const buffer = await res.arrayBuffer();
  const mime = res.headers.get("content-type") || "application/octet-stream";
  const b64 = Buffer.from(buffer).toString("base64");
  return `data:${mime};base64,${b64}`;
}

async function airtableCreateRow(fields) {
  const body = { records: [{ fields }] };
  const res = await fetch(AIRTABLE_API_ROOT, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Airtable create failed: ${res.status} ${txt}`);
  }
  const data = await res.json();
  return data.records[0];
}

async function airtableUpdateRecord(recordId, fields) {
  const url = `${AIRTABLE_API_ROOT}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Airtable update failed: ${res.status} ${txt}`);
  }
  return res.json();
}

function genId(prefix = "run") {
  return `${prefix}_${Date.now()}_${crypto.randomBytes(4).toString("hex")}`;
}

/* ---------- WaveSpeed helpers (assumptions - adapt if your API differs) ---------- */

/**
 * submitToWaveSpeed(dataURLs, prompt, size)
 * - dataURLs: [subjectDataURL, ref1DataURL, ref2DataURL...]
 * - returns { requestId }
 *
 * NOTE: adapt the request body structure to your Seedream v4 API.
 */
async function submitToWaveSpeed({ dataURLs, prompt, width, height, model = "seedream-v4" }) {
  // sample payload - adjust to your API
  const payload = {
    model,
    prompt,
    images: dataURLs, // subject first, refs after
    width,
    height,
    // other generation options can be added here
    webhook: `${PUBLIC_BASE_URL.replace(/\/$/, "")}/webhooks/wavespeed`,
    // optional: add a client-provided meta id to help match callbacks
    client_meta: { submitted_at: new Date().toISOString() },
  };

  const res = await fetch(WAVESPEED_SUBMIT_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${WAVESPEED_API_KEY}`,
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`WaveSpeed submit failed: ${res.status} ${txt}`);
  }
  const json = await res.json();
  // expected: { request_id: "..." } or similar
  // adapt field name if necessary
  const requestId = json.request_id || json.id || json.requestId || json.requestID;
  if (!requestId) throw new Error("WaveSpeed response did not include a request id. Response: " + JSON.stringify(json));
  return { requestId, raw: json };
}

/**
 * pollWaveSpeedStatus(requestId)
 * - returns { status: 'processing'|'completed'|'failed', outputs: [url...], info: raw }
 * 
 * If your API uses a different endpoint/shape, adapt here.
 */
async function pollWaveSpeedStatus(requestId) {
  const url = `${WAVESPEED_POLL_URL}/${encodeURIComponent(requestId)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${WAVESPEED_API_KEY}`, "Content-Type": "application/json" },
  });
  if (res.status === 404) {
    return { status: "failed", outputs: [], info: { error: "not_found" } };
  }
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`WaveSpeed poll failed: ${res.status} ${txt}`);
  }
  const json = await res.json();
  // expected shape: { status: 'succeeded'|'processing'|'failed', outputs: [{ url }] }
  const statusRaw = (json.status || "").toLowerCase();
  let status = "processing";
  if (statusRaw === "succeeded" || statusRaw === "completed" || statusRaw === "done") status = "completed";
  if (statusRaw === "failed" || statusRaw === "error") status = "failed";
  // Try to extract output URLs
  const outputs = [];
  if (Array.isArray(json.outputs)) {
    for (const o of json.outputs) {
      if (o.url) outputs.push(o.url);
      else if (o.uri) outputs.push(o.uri);
      else if (o.b64) {
        // If it's base64, we cannot attach directly to Airtable; would need to host it.
        // We'll encode as data URL; note Airtable may not accept data: URLs for attachments.
        outputs.push(`data:image/png;base64,${o.b64}`);
      }
    }
  }
  return { status, outputs, info: json };
}

/* ---------- Reliability helpers: retries + backoff ---------- */

async function retry(fn, { retries = 5, baseMs = 500, factor = 1.8 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await fn(attempt);
    } catch (err) {
      attempt++;
      if (attempt > retries) throw err;
      const wait = Math.round(baseMs * Math.pow(factor, attempt - 1));
      console.warn(`Retry ${attempt}/${retries} after err: ${err.message}. Waiting ${wait}ms`);
      await sleep(wait);
    }
  }
}

/* ---------- In-memory index (short-lived) ---------- */
/* We also persist to Airtable so this is just an accelerator during runtime. */

const inFlightRequests = new Map(); // requestId => { airtableRecordId, batchRunId }

/* ---------- /app form (simple) ---------- */

app.get("/", (req, res) => res.send("OK"));
app.get("/app", (req, res) => {
  res.send(`<!doctype html>
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Seedream Batch</title>
    <style>
      body{font-family:system-ui,Segoe UI,Roboto;max-width:900px;margin:24px auto;padding:12px}
      label{display:block;margin-top:12px}
      input[type=text], textarea {width:100%;padding:8px;border-radius:6px;border:1px solid #ddd}
      .row{display:flex;gap:10px}
      .col{flex:1}
      button{margin-top:12px;padding:10px 16px;border-radius:8px;border:0;background:#2563eb;color:#fff}
    </style>
  </head>
  <body>
    <h1>Seedream Batch</h1>
    <form method="POST" action="/generate-batch">
      <label>Prompt<textarea name="prompt" rows="4" required></textarea></label>
      <label>Subject image URL<input type="text" name="subject" required placeholder="https://..." /></label>
      <label>Reference image URLs (comma separated)<input type="text" name="refs" placeholder="https://..., https://..." /></label>
      <div class="row">
        <div class="col"><label>Width<input type="text" name="width" placeholder="2227" /></label></div>
        <div class="col"><label>Height<input type="text" name="height" placeholder="3961" /></label></div>
      </div>
      <label>Batch count<input type="text" name="batch" value="4" /></label>
      <label>Model<input type="text" name="model" placeholder="seedream-v4" /></label>
      <button type="submit">Generate</button>
    </form>
    <p style="font-size:12px;color:#666">This server will create one Airtable row per batch and track outputs.</p>
  </body>
  </html>`);
});

/* ---------- /generate-batch handler ---------- */

app.post("/generate-batch", async (req, res) => {
  try {
    const prompt = (req.body.prompt || "").trim();
    const subjectUrl = (req.body.subject || "").trim();
    const refsRaw = (req.body.refs || "").trim();
    const width = parseInt(req.body.width, 10) || undefined;
    const height = parseInt(req.body.height, 10) || undefined;
    const batch = Math.max(1, Math.min(50, parseInt(req.body.batch, 10) || 4));
    const model = req.body.model || "seedream-v4";

    if (!prompt || !subjectUrl) return res.status(400).send("Prompt + subject required.");

    // build data URLs for subject and refs (subject first)
    const refUrls = refsRaw ? refsRaw.split(",").map(s => s.trim()).filter(Boolean) : [];
    const allImageUrls = [subjectUrl, ...refUrls];

    // Fetch and convert to data URLs with retries
    const dataURLs = [];
    for (const src of allImageUrls) {
      const dataUrl = await retry(() => fetchImageAsDataURL(src), { retries: 3 });
      dataURLs.push(dataUrl);
    }

    // Create parent Airtable row for this batch
    const runId = genId("run");
    const createdAt = new Date().toISOString();
    const initialFields = {
      Prompt: prompt,
      "Status": "processing",
      "Model": model,
      "Size": `${width || ""}x${height || ""}`,
      "Request IDs": "", // will fill
      "Seen IDs": "",
      "Failed IDs": "",
      "Run ID": runId,
      "Created at": createdAt,
      "Last Update": createdAt,
    };
    const airtableRecord = await airtableCreateRow(initialFields);
    const airtableRecordId = airtableRecord.id;

    // Prepare to submit N jobs, spacing them ~1200ms
    const requestIds = [];
    const seenIds = [];
    const failedIds = [];

    for (let i = 0; i < batch; i++) {
      // Each job gets same prompt + images. Optionally you could randomize or add guidance per job.
      const attemptSubmit = async () => {
        const { requestId } = await submitToWaveSpeed({
          dataURLs,
          prompt,
          width,
          height,
          model,
        });
        return requestId;
      };

      // Retry submit with backoff
      try {
        const requestId = await retry(() => attemptSubmit(), { retries: 4, baseMs: 600 });
        requestIds.push(requestId);
        // cache association for poller and webhooks
        inFlightRequests.set(requestId, { airtableRecordId, runId });
      } catch (err) {
        console.error("Submit failed for one job:", err.message);
        // record "failed" request (no requestId)
        const pseudoId = `submit_failed_${i}_${Date.now()}`;
        failedIds.push(pseudoId);
      }

      // spacing to avoid rate limits
      await sleep(1200);
    }

    // Update Airtable row with Request IDs
    const now = new Date().toISOString();
    await airtableUpdateRecord(airtableRecordId, {
      "Request IDs": requestIds.join(","),
      "Seen IDs": seenIds.join(","),
      "Failed IDs": failedIds.join(","),
      "Last Update": now,
    });

    // Immediately respond with run info and Airtable row link (if PUBLIC_BASE_URL present)
    const appUrl = PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL.replace(/\/$/, "")}/app` : "";
    res.send({
      ok: true,
      airtableRecordId,
      runId,
      requestIds,
      preview: appUrl,
      message: "Batch submitted; Airtable row created with Status=processing.",
    });
  } catch (err) {
    console.error("generate-batch err:", err);
    res.status(500).send({ error: err.message });
  }
});

/* ---------- Webhook receiver for WaveSpeed callbacks ---------- */
/* Expected payload (example):
   {
     request_id: "...",
     status: "succeeded"|"failed"|"processing",
     outputs: [{ url: "..."}]
   }
   Adapt if your provider sends different shape.
*/

app.post("/webhooks/wavespeed", async (req, res) => {
  try {
    const body = req.body || {};
    // Accept either request_id or id
    const requestId = body.request_id || body.id || body.requestId;
    if (!requestId) {
      res.status(400).send("Missing request_id");
      return;
    }

    const { status = "processing", outputs = [] } = body;
    // find the associated Airtable row
    const meta = inFlightRequests.get(requestId);
    if (!meta) {
      // we don't have it in memory; attempt to find Airtable row by searching 'Request IDs' field
      // this is an expensive fallback but useful if server restarted
      const rec = await findAirtableRecordByRequestId(requestId);
      if (!rec) {
        console.warn("Webhook for unknown requestId", requestId);
        // still respond 200 so sender won't retry excessively
        res.send({ ok: true });
        return;
      }
      meta = { airtableRecordId: rec.id, runId: rec.fields["Run ID"] };
    }

    const airtableRecordId = meta.airtableRecordId;

    // Append outputs to Output field and set Output URL (first) maybe
    const outputUrls = outputs.map(o => o.url || o.uri).filter(Boolean);

    // Get existing record to merge fields
    const currentRec = await fetch(`${AIRTABLE_API_ROOT}/${airtableRecordId}`, {
      headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
    }).then((r) => r.json());

    const existingFields = currentRec.fields || {};
    const existingOutputs = existingFields.Output || []; // Airtable attachment array
    const existingOutputUrls = existingOutputs.map(x => x.url).filter(Boolean);

    // Append new outputs
    const attachmentsToAdd = outputUrls
      .filter(u => !existingOutputUrls.includes(u))
      .map(u => ({ url: u }));

    // Build Seen IDs and Failed IDs updates
    const prevSeen = (existingFields["Seen IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);
    const prevFailed = (existingFields["Failed IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);
    const prevRequestIds = (existingFields["Request IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);

    let newSeen = prevSeen.slice();
    let newFailed = prevFailed.slice();

    if (status === "completed" || status === "succeeded" || status === "done") {
      newSeen.push(requestId);
    } else if (status === "failed" || status === "error") {
      newFailed.push(requestId);
      newSeen.push(requestId);
    } else {
      // processing -> mark seen but not final
      newSeen.push(requestId);
    }

    // Deduplicate
    newSeen = Array.from(new Set(newSeen));
    newFailed = Array.from(new Set(newFailed));

    // If all Request IDs are seen or failed, mark run completed
    const allRequestIds = prevRequestIds;
    const allSeenOrFailed = allRequestIds.length > 0 && allRequestIds.every(id => newSeen.includes(id) || newFailed.includes(id));

    const updateFields = {
      "Seen IDs": newSeen.join(","),
      "Failed IDs": newFailed.join(","),
      "Last Update": new Date().toISOString(),
    };

    if (attachmentsToAdd.length) {
      // Airtable attachments are provided in "Output" field as array of { url }
      // We PATCH by setting Output to existing + attachmentsToAdd
      updateFields["Output"] = [...existingOutputs, ...attachmentsToAdd];
      // Also save first output URL into Output URL (useful quick-link)
      const allOutputsFinal = [...existingOutputUrls, ...attachmentsToAdd.map(a => a.url)];
      if (allOutputsFinal.length) updateFields["Output URL"] = allOutputsFinal[0];
    }

    if (allSeenOrFailed) {
      updateFields["Status"] = "completed";
      updateFields["Completed At"] = new Date().toISOString();
    }

    await airtableUpdateRecord(airtableRecordId, updateFields);

    // remove from in-flight map if finished
    if (allSeenOrFailed) {
      allRequestIds.forEach(id => inFlightRequests.delete(id));
    } else {
      inFlightRequests.delete(requestId); // still remove this id so memory doesn't grow; poller will handle missing ones
    }

    res.send({ ok: true });
  } catch (err) {
    console.error("webhook error", err);
    // respond 200 anyway to avoid excessive retries unless you want retries
    res.status(200).send({ ok: false, error: err.message });
  }
});

/* ---------- Poller: periodically check requests that haven't been finalized ---------- */

const POLL_INTERVAL_MS = 15_000; // 15s
const MAX_POLL_ATTEMPTS = 20; // per request before marking failed (configurable)
const requestPollInfo = new Map(); // requestId => { attempts, airtableRecordId, runId }

async function findAirtableRecordByRequestId(requestId) {
  // Search Airtable for Request IDs containing this requestId.
  // This uses filterByFormula and may require escaping.
  try {
    const filter = encodeURIComponent(`FIND("${requestId}",{Request IDs})`);
    const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE}?filterByFormula=${filter}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` } });
    if (!res.ok) return null;
    const json = await res.json();
    if (Array.isArray(json.records) && json.records.length > 0) {
      return json.records[0];
    }
    return null;
  } catch (err) {
    console.error("findAirtableRecordByRequestId err", err);
    return null;
  }
}

async function pollerLoop() {
  try {
    // Find Airtable rows in "processing" status with Request IDs
    // We'll page through results (simple approach: single page up to 100 records)
    const filter = encodeURIComponent(`{Status} = 'processing'`);
    const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE}?pageSize=100&filterByFormula=${filter}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` } });
    if (!res.ok) {
      console.warn("Poller: failed to fetch processing rows", res.status);
      return;
    }
    const json = await res.json();
    const records = json.records || [];
    for (const rec of records) {
      const recId = rec.id;
      const fields = rec.fields || {};
      const reqIds = (fields["Request IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);
      const seen = (fields["Seen IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);
      const failed = (fields["Failed IDs"] || "").split(",").map(s => s.trim()).filter(Boolean);

      for (const rid of reqIds) {
        // If already seen or failed, skip
        if (seen.includes(rid) || failed.includes(rid)) continue;

        // poll each request
        try {
          const pollResp = await retry(() => pollWaveSpeedStatus(rid), { retries: 2, baseMs: 600 });
          if (pollResp.status === "completed") {
            // store outputs to Airtable
            const outputs = pollResp.outputs || [];
            const att = outputs.map(u => ({ url: u }));
            const now = new Date().toISOString();

            // Append attachments
            const existingOutputs = (fields.Output || []).map(x => x.url).filter(Boolean);
            const toAdd = att.filter(a => !existingOutputs.includes(a.url));
            const updateFields = {
              "Output": [...(fields.Output || []), ...toAdd],
              "Output URL": (fields["Output URL"] || toAdd[0]?.url),
              "Seen IDs": (fields["Seen IDs"] ? fields["Seen IDs"] + "," : "") + rid,
              "Last Update": now,
            };

            // Are all done?
            const newSeen = ((fields["Seen IDs"] || "").split(",").map(s => s.trim()).filter(Boolean)).concat([rid]);
            const allSeenOrFailed = reqIds.every(id => newSeen.includes(id) || failed.includes(id));
            if (allSeenOrFailed) {
              updateFields["Status"] = "completed";
              updateFields["Completed At"] = now;
            }

            await airtableUpdateRecord(recId, updateFields);

            // remove from inFlightRequests
            inFlightRequests.delete(rid);
          } else if (pollResp.status === "failed") {
            // mark as failed
            const now = new Date().toISOString();
            const updateFields = {
              "Failed IDs": (fields["Failed IDs"] ? fields["Failed IDs"] + "," : "") + rid,
              "Seen IDs": (fields["Seen IDs"] ? fields["Seen IDs"] + "," : "") + rid,
              "Last Update": now,
            };
            // check complete
            const newFailed = ((fields["Failed IDs"] || "").split(",").map(s => s.trim()).filter(Boolean)).concat([rid]);
            const newSeen = ((fields["Seen IDs"] || "").split(",").map(s => s.trim()).filter(Boolean)).concat([rid]);
            const allSeenOrFailed = reqIds.every(id => newSeen.includes(id) || newFailed.includes(id));
            if (allSeenOrFailed) {
              updateFields["Status"] = "completed";
              updateFields["Completed At"] = now;
            }
            await airtableUpdateRecord(recId, updateFields);
            inFlightRequests.delete(rid);
          } else {
            // still processing; increment attempts and maybe timeout after many tries
            const info = requestPollInfo.get(rid) || { attempts: 0, airtableRecordId: recId };
            info.attempts = (info.attempts || 0) + 1;
            requestPollInfo.set(rid, info);
            if (info.attempts > MAX_POLL_ATTEMPTS) {
              // give up, mark as failed (timeout)
              const now = new Date().toISOString();
              const updateFields = {
                "Failed IDs": (fields["Failed IDs"] ? fields["Failed IDs"] + "," : "") + rid,
                "Seen IDs": (fields["Seen IDs"] ? fields["Seen IDs"] + "," : "") + rid,
                "Last Update": now,
              };
              const newFailed = ((fields["Failed IDs"] || "").split(",").map(s => s.trim()).filter(Boolean)).concat([rid]);
              const newSeen = ((fields["Seen IDs"] || "").split(",").map(s => s.trim()).filter(Boolean)).concat([rid]);
              const allSeenOrFailed = reqIds.every(id => newSeen.includes(id) || newFailed.includes(id));
              if (allSeenOrFailed) {
                updateFields["Status"] = "completed";
                updateFields["Completed At"] = now;
              }
              await airtableUpdateRecord(recId, updateFields);
              inFlightRequests.delete(rid);
            }
          }
        } catch (pollErr) {
          console.warn("Poll err for", rid, pollErr.message);
          // continue; retry next tick
        } finally {
          // small sleep to respect rate limiting
          await sleep(250);
        }
      } // end reqIds loop
    } // end records loop
  } catch (err) {
    console.error("pollerLoop err", err);
  } finally {
    // schedule next
    setTimeout(pollerLoop, POLL_INTERVAL_MS);
  }
}

// start poller after server boots
setTimeout(pollerLoop, 3_000);

/* ---------- Start server ---------- */

app.listen(PORT, () => {
  console.log(`Seedream Airtable app listening on port ${PORT}`);
  console.log(`PUBLIC_BASE_URL=${PUBLIC_BASE_URL}`);
  console.log(`Make sure WAVESPEED webhook is allowed to call ${PUBLIC_BASE_URL}/webhooks/wavespeed`);
});
