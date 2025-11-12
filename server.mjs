// server.mjs
import express from "express";
import bodyParser from "body-parser";
import { randomUUID } from "crypto";
import fs from "fs/promises";
import path from "path";

// --- Config: env vars ---
const PORT = process.env.PORT || 3000;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "https://MY-SERVER";
const WAVESPEED_API_KEY = process.env.WAVESPEED_API_KEY;
const WAVESPEED_API_URL = process.env.WAVESPEED_API_URL || "https://api.wavespeed.ai/v1/generate"; // override as needed
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const AIRTABLE_TABLE = process.env.AIRTABLE_TABLE || "Generations";

if (!WAVESPEED_API_KEY) {
  console.warn("WARNING: WAVESPEED_API_KEY not set in env - WaveSpeed calls will fail.");
}
if (!AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) {
  console.warn("WARNING: Airtable credentials missing - Airtable calls will fail.");
}

// --- Utilities ---
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function exponentialBackoff(attempt, base = 500) {
  // jittered exponential backoff
  const jitter = Math.floor(Math.random() * 200);
  return base * Math.pow(2, attempt) + jitter;
}

// Convert an image URL to a base64 data URL
async function fetchImageAsDataURL(url) {
  const res = await fetch(url, { method: "GET" });
  if (!res.ok) throw new Error(`Failed to fetch image ${url}: ${res.status}`);
  const contentType = res.headers.get("content-type") || "application/octet-stream";
  const buffer = await res.arrayBuffer();
  const b64 = Buffer.from(buffer).toString("base64");
  return `data:${contentType};base64,${b64}`;
}

// Send HTTP with retries (generic)
async function httpWithRetries(url, options = {}, attempts = 3) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await fetch(url, options);
      const text = await res.text();
      let json;
      try { json = JSON.parse(text); } catch (e) { json = text; }
      if (!res.ok) {
        throw Object.assign(new Error(`HTTP ${res.status}: ${JSON.stringify(json)}`), { status: res.status, body: json });
      }
      return json;
    } catch (err) {
      lastErr = err;
      const wait = exponentialBackoff(i, 600);
      await sleep(wait);
    }
  }
  throw lastErr;
}

// --- Airtable helpers ---
const AIRTABLE_BASE_URL = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

// Create a parent row for the batch
async function airtableCreateParentRow(fields = {}) {
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) return null;
  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(AIRTABLE_TABLE)}`;
  const body = { fields };
  const res = await httpWithRetries(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, 3);
  // res will include id and fields
  return res.records ? res.records[0] : res;
}

// Update fields on a record (patch)
async function airtableUpdateRecord(recordId, fields) {
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) return null;
  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(AIRTABLE_TABLE)}/${recordId}`;
  const body = { fields };
  const res = await httpWithRetries(url, {
    method: "PATCH",
    headers: {
      "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, 3);
  return res;
}

// Get record
async function airtableGetRecord(recordId) {
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) return null;
  const url = `${AIRTABLE_BASE_URL}/${encodeURIComponent(AIRTABLE_TABLE)}/${recordId}`;
  const res = await httpWithRetries(url, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
  }, 3);
  return res;
}

// Upload base64 file to a specific attachment field (upload-attachment endpoint)
async function airtableUploadAttachment(recordId, fieldName, filename, base64Data) {
  // Airtable upload-attachment expects multipart/form-data with file. But they support base64 via dedicated endpoint:
  // POST https://content.airtable.com/v0/{baseId}/{recordId}/{attachmentFieldIdOrName}/uploadAttachment
  if (!AIRTABLE_TOKEN || !AIRTABLE_BASE_ID) return null;
  const url = `https://content.airtable.com/v0/${AIRTABLE_BASE_ID}/${recordId}/${encodeURIComponent(fieldName)}/uploadAttachment`;
  // The endpoint accepts a form field "file" as base64; however, to keep things simple here we'll send JSON
  // with { "file": "<base64>" } if supported. If not, you can change to multipart form upload.
  // Many Airtable instances support a "file" param as base64 in JSON payload for upload-attachment endpoint.
  const payload = { file: base64Data, filename };
  const res = await httpWithRetries(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  }, 3);
  return res;
}

// Append output URLs into Output (attachments) field by sending {attachments: [{url, filename}, ...]}
// Airtable will fetch and copy those URLs into its attachments.
async function airtableAppendOutputs(recordId, outputs) {
  // outputs: array of { url, filename }
  // To append we need to fetch the record's current Output attachments and combine.
  const rec = await airtableGetRecord(recordId);
  const current = (rec && rec.fields && rec.fields.Output) || [];
  const newFields = { Output: [...current, ...outputs] };
  // Also set Output URL to first output url if not set
  if (!rec.fields.Output || rec.fields.Output.length === 0) {
    newFields["Output URL"] = outputs[0]?.url || "";
  }
  return airtableUpdateRecord(recordId, newFields);
}

// Utility to append text to long-text fields like Request IDs, Seen IDs, Failed IDs (comma separated).
async function airtableAppendCommaField(recordId, fieldName, valueToAdd) {
  const rec = await airtableGetRecord(recordId);
  const current = (rec && rec.fields && rec.fields[fieldName]) || "";
  const arr = current ? String(current).split(",").map(s => s.trim()).filter(Boolean) : [];
  if (!arr.includes(valueToAdd)) arr.push(valueToAdd);
  const toSet = arr.join(", ");
  await airtableUpdateRecord(recordId, { [fieldName]: toSet });
  return toSet;
}

// --- WaveSpeed helpers ---
// Submit a job; subjectDataUrl is data:... base64, refsDataUrls array
async function wavespeedSubmitJob({ prompt, subjectDataUrl, refsDataUrls = [], width = 1024, height = 1024, model = "seedream-v4", runId, batchIndex, webhookUrl }) {
  // Construct payload assuming WaveSpeed expects images as data URLs in fields like "images" or "image_urls".
  // Because WaveSpeed endpoints differ across integrations, we send a reasonable JSON body:
  const body = {
    model,
    prompt,
    width,
    height,
    images: [subjectDataUrl, ...refsDataUrls], // subject first
    meta: { runId, batchIndex },
    // We may include a flag to request base64 output; leave it optional
    // enable_base64_output: false,
  };

  // Append webhook param to request URL
  const url = `${WAVESPEED_API_URL}?webhook=${encodeURIComponent(webhookUrl)}`;

  const res = await httpWithRetries(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${WAVESPEED_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  }, 3);

  // Expected response contains an ID or request_id (different providers vary).
  // We'll try to extract a commonly-named field:
  const requestId = res?.id || res?.request_id || res?.requestId || (res?.data && res.data.id) || null;
  return { raw: res, requestId };
}

// Poll for job result until terminal or timeout
async function pollUntilDone(requestId, parentRecordId, opts = {}) {
  const pollInterval = opts.pollInterval || 7000; // 7s
  const timeoutMs = opts.timeoutMs || 20 * 60 * 1000; // 20 minutes
  const start = Date.now();

  const resultEndpoint = `${WAVESPEED_API_URL.replace(/\/generate\/?$/, "/status")}`; // attempt - users may override WAVESPEED_API_URL

  async function attemptPoll() {
    try {
      // If the provider exposes a dedicated status endpoint (common pattern)
      // We'll try several endpoint shapes. Providers differ; adapt if needed.
      const endpointsToTry = [
        `${resultEndpoint}/${encodeURIComponent(requestId)}`,
        `${WAVESPEED_API_URL}/status/${encodeURIComponent(requestId)}`,
        `${WAVESPEED_API_URL}/${encodeURIComponent(requestId)}`,
      ];
      let json = null;
      for (const ep of endpointsToTry) {
        try {
          json = await httpWithRetries(ep, {
            method: "GET",
            headers: { "Authorization": `Bearer ${WAVESPEED_API_KEY}` },
          }, 2);
          if (json) break;
        } catch (e) {
          // try next
        }
      }
      if (!json) {
        throw new Error("No status JSON from any status endpoint.");
      }

      // Interpret status: providers usually return status: pending/running/completed/failed
      const status = json.status || json.state || (json.data && json.data.status) || "unknown";
      if (/completed|succeeded|finished/i.test(status)) {
        // Extract outputs (list of URLs or base64)
        const outputs = json.output || json.outputs || json.data?.output || json.data?.outputs || [];
        // Normalize to array of {url, filename}
        const normalized = (Array.isArray(outputs) ? outputs : [outputs]).filter(Boolean).map((o, idx) => {
          if (typeof o === "string") return { url: o, filename: `output-${requestId}-${idx}.png` };
          if (o.url) return { url: o.url, filename: o.filename || `output-${requestId}-${idx}.png` };
          return null;
        }).filter(Boolean);
        return { status: "completed", outputs: normalized, raw: json };
      } else if (/failed|error/i.test(status)) {
        return { status: "failed", reason: json.error || json };
      } else {
        // still running
        return { status: "running" };
      }
    } catch (err) {
      return { status: "error", error: String(err) };
    }
  }

  // Poll loop with light backoff
  while (Date.now() - start < timeoutMs) {
    const r = await attemptPoll();
    if (r.status === "completed") {
      // handle success: append outputs and mark seenId
      await handleJobCompletion(requestId, parentRecordId, r.outputs, false);
      return r;
    }
    if (r.status === "failed") {
      await handleJobCompletion(requestId, parentRecordId, [], true, r.reason || "failed");
      return r;
    }
    // not terminal: sleep then continue
    await sleep(pollInterval);
  }

  // Timeout
  await handleJobCompletion(requestId, parentRecordId, [], true, "timeout");
  return { status: "timeout" };
}

// Called when a job finishes (either by webhook or polling)
async function handleJobCompletion(requestId, parentRecordId, outputs = [], failed = false, reason = "") {
  // Append to Seen IDs
  await airtableAppendCommaField(parentRecordId, "Seen IDs", requestId);

  if (failed) {
    await airtableAppendCommaField(parentRecordId, "Failed IDs", requestId);
    // Optionally add reason in a hidden field or a notes field - omitted
  } else if (outputs && outputs.length) {
    // Append outputs into Output attachment field so Airtable will fetch those URLs.
    const normalized = outputs.map((o) => ({ url: o.url, filename: o.filename || path.basename(new URL(o.url).pathname) }));
    await airtableAppendOutputs(parentRecordId, normalized);
  }

  // After update, fetch Request IDs and Seen IDs and possibly flip status
  const rec = await airtableGetRecord(parentRecordId);
  const requestIds = (rec && rec.fields && rec.fields["Request IDs"]) ? String(rec.fields["Request IDs"]).split(",").map(s => s.trim()).filter(Boolean) : [];
  const seenIds = (rec && rec.fields && rec.fields["Seen IDs"]) ? String(rec.fields["Seen IDs"]).split(",").map(s => s.trim()).filter(Boolean) : [];
  if (requestIds.length > 0) {
    const allSeen = requestIds.every(id => seenIds.includes(id));
    if (allSeen) {
      await airtableUpdateRecord(parentRecordId, { Status: "completed", "Completed At": new Date().toISOString() });
    } else {
      // keep status processing unless all seen
      await airtableUpdateRecord(parentRecordId, { Status: "processing", "Last Update": new Date().toISOString() });
    }
  }
}

// --- Express app and routes ---
const app = express();
app.use(bodyParser.urlencoded({ extended: true, limit: "10mb" }));
app.use(bodyParser.json({ limit: "50mb" }));

// Minimal UI
app.get("/app", (req, res) => {
  res.type("html").send(`
<!doctype html>
<html>
<head><meta charset="utf-8"><title>WaveSpeed Batch Runner</title></head>
<body>
  <h2>WaveSpeed Batch Runner</h2>
  <form method="POST" action="/start">
    <label>Prompt:<br><textarea name="prompt" rows="4" cols="60"></textarea></label><br>
    <label>Subject image URL:<br><input name="subject" size="80"></label><br>
    <label>Reference image URLs (comma-separated):<br><input name="refs" size="80"></label><br>
    <label>Width: <input name="width" value="1024"></label>
    <label>Height: <input name="height" value="1024"></label><br>
    <label>Batch count: <input name="batchCount" value="1"></label><br>
    <label>Model: <input name="model" value="seedream-v4"></label><br>
    <button type="submit">Start Batch</button>
  </form>
</body>
</html>
  `);
});

// Start a batch: convert images -> base64, create Airtable parent row, submit jobs spaced ~1.2s
app.post("/start", async (req, res) => {
  try {
    const { prompt, subject, refs = "", width = 1024, height = 1024, batchCount = 1, model = "seedream-v4" } = req.body;
    const refsArr = String(refs || "").split(",").map(s => s.trim()).filter(Boolean);
    const runId = randomUUID();
    // Create Airtable parent row with initial fields
    const createdAt = new Date().toISOString();
    const parentInitFields = {
      Prompt: prompt || "",
      Model: model,
      Size: `${width}x${height}`,
      Status: "processing",
      "Run ID": runId,
      "Created At": createdAt,
      "Last Update": createdAt,
      "Request IDs": "",
      "Seen IDs": "",
      "Failed IDs": "",
    };
    const parent = await airtableCreateParentRow(parentInitFields);
    const parentRecordId = parent?.id || (parent?.records && parent.records[0]?.id);
    if (!parentRecordId) {
      console.warn("Failed to create parent row in Airtable - continuing but parentRecordId missing.");
    }

    // Convert subject and refs to base64 data urls (for WaveSpeed)
    const subjectDataUrl = await fetchImageAsDataURL(subject);
    const refsDataUrls = [];
    for (const r of refsArr) {
      try {
        const d = await fetchImageAsDataURL(r);
        refsDataUrls.push(d);
      } catch (e) {
        console.warn("Failed to fetch ref:", r, e);
      }
    }

    // Submit batchCount jobs spaced by ~1.2s
    const submittedIds = [];
    for (let i = 0; i < Number(batchCount); i++) {
      // submit
      try {
        const webhookUrl = `${PUBLIC_BASE_URL.replace(/\/$/, "")}/webhooks/wavespeed`;
        const { requestId } = await wavespeedSubmitJob({
          prompt,
          subjectDataUrl,
          refsDataUrls,
          width: Number(width),
          height: Number(height),
          model,
          runId,
          batchIndex: i,
          webhookUrl,
        });
        const rid = requestId || `unknown-${randomUUID()}`;
        submittedIds.push(rid);
        // Add Request ID immediately to Airtable
        if (parentRecordId) {
          await airtableAppendCommaField(parentRecordId, "Request IDs", rid);
        }
        // Start polling in background (not awaited here) — pollUntilDone will update Airtable on completion
        pollUntilDone(rid, parentRecordId).catch(e => {
          console.error("pollUntilDone error:", e);
        });
      } catch (err) {
        console.error("Submit job failed:", err);
        // record failed immediately
        if (parentRecordId) await airtableAppendCommaField(parentRecordId, "Failed IDs", `submit-failed-${i}`);
      }
      // Wait ~1.2s between job submissions
      await sleep(1200);
    }

    // Return immediate response with parent row id and request ids
    res.json({ ok: true, parentRecordId, requestIds: submittedIds, runId });
  } catch (err) {
    console.error("Start batch error:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Webhook handler (WaveSpeed will POST here with job results)
app.post("/webhooks/wavespeed", async (req, res) => {
  // Accept payloads that include requestId/id and outputs
  // Normalize body
  const body = req.body || {};
  const requestId = body.request_id || body.id || body.requestId || body.data?.id || body?.meta?.requestId;
  const parentRunId = body?.meta?.runId || body?.runId || body?.data?.meta?.runId;
  // outputs: either an array of URLs or {url, filename}
  let outputs = body.output || body.outputs || body.data?.output || body.data?.outputs || [];
  if (!Array.isArray(outputs)) outputs = [outputs];

  // If the webhook included a runId, try to find the parent Airtable row by Run ID (search)
  let parentRecordId = null;
  try {
    if (parentRunId && AIRTABLE_TOKEN && AIRTABLE_BASE_ID) {
      const q = `${AIRTABLE_BASE_URL}/${encodeURIComponent(AIRTABLE_TABLE)}?filterByFormula=${encodeURIComponent(`{Run ID} = "${parentRunId}"`)}`;
      const found = await httpWithRetries(q, {
        method: "GET",
        headers: { "Authorization": `Bearer ${AIRTABLE_TOKEN}` },
      }, 2);
      const rec = (found && found.records && found.records[0]);
      if (rec) parentRecordId = rec.id;
    }
  } catch (e) {
    console.warn("Webhook parent lookup failed:", e);
  }

  // If runId lookup failed, maybe webhook includes parentRecordId directly in meta
  if (!parentRecordId && body.meta && body.meta.parentRecordId) parentRecordId = body.meta.parentRecordId;

  // Normalize outputs into {url, filename}
  const normalized = outputs.filter(Boolean).map((o, idx) => {
    if (typeof o === "string") return { url: o, filename: `webhook-${requestId}-${idx}.png` };
    if (o.url) return { url: o.url, filename: o.filename || `webhook-${requestId}-${idx}.png` };
    return null;
  }).filter(Boolean);

  // Append outputs and Seen IDs
  try {
    if (parentRecordId) {
      if (normalized.length) await airtableAppendOutputs(parentRecordId, normalized);
      if (requestId) await airtableAppendCommaField(parentRecordId, "Seen IDs", requestId);
      // If the webhook signals failure, append to Failed IDs
      const status = body.status || body.state || body.data?.status || "";
      if (/fail|error/i.test(status) || body.error) {
        if (requestId) await airtableAppendCommaField(parentRecordId, "Failed IDs", requestId);
      }
      // Possibly flip status if all seen — handled by handleJobCompletion
      await handleJobCompletion(requestId, parentRecordId, normalized, /fail|error/i.test(body.status || "") || Boolean(body.error));
    } else {
      console.warn("Webhook: parentRecordId unknown; cannot attach outputs to Airtable.");
    }
  } catch (err) {
    console.error("Webhook handling error:", err);
  }

  // Respond 200 quickly
  res.status(200).json({ ok: true });
});

// Health
app.get("/", (req, res) => res.json({ ok: true, now: new Date().toISOString() }));

// Start server
app.listen(PORT, () => {
  console.log(`Server listening on ${PORT} - UI at /app`);
});
