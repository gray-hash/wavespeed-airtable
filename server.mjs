// server.mjs
import express from "express";
import fetch from "node-fetch"; // If Node 18+, global fetch exists; node-fetch is safe fallback.
import dotenv from "dotenv";
import bodyParser from "body-parser";
import crypto from "crypto";
import { URL } from "url";
import { setTimeout as wait } from "timers/promises";

dotenv.config();

const {
  PORT = 3000,
  PUBLIC_BASE_URL = "http://localhost:3000",
  WAVESPEED_API_KEY,
  WAVESPEED_API_BASE = "https://api.wavespeed.example/v4", // replace with real base
  AIRTABLE_TOKEN,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE,
} = process.env;

if (!WAVESPEED_API_KEY || !AIRTABLE_TOKEN || !AIRTABLE_BASE_ID || !AIRTABLE_TABLE) {
  console.warn("Warning: One or more environment variables are missing. See README in comments.");
}

const app = express();
app.use(bodyParser.json({ limit: "10mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "10mb" }));

// Simple HTML UI at /app
app.get("/app", (req, res) => {
  res.type("html").send(`<!doctype html>
  <html>
  <head><meta charset="utf-8"><title>WaveSpeed Batch Runner</title></head>
  <body style="font-family:system-ui, -apple-system, sans-serif; padding:2rem">
    <h1>WaveSpeed Batch Runner</h1>
    <form method="POST" action="/start-batch">
      <label>Prompt<br><textarea name="prompt" rows="3" cols="60" required></textarea></label><br><br>
      <label>Subject Image URL<br><input name="subjectUrl" type="url" style="width:60%" required /></label><br><br>
      <label>Reference Image URLs (comma-separated)<br><input name="refUrls" type="text" style="width:60%" /></label><br><br>
      <label>Width <input name="width" type="number" value="512" /></label>
      <label>Height <input name="height" type="number" value="512" /></label><br><br>
      <label>Batch Count <input name="batchCount" type="number" value="3" min="1" /></label><br><br>
      <button type="submit">Start Batch</button>
    </form>
    <p>Webhook URL: <code>${PUBLIC_BASE_URL}/webhooks/wavespeed</code></p>
  </body>
  </html>`);
});

/**
 * Utilities
 */

// Basic exponential backoff helper
async function retryWithBackoff(fn, attempts = 3, initialDelayMs = 500) {
  let lastErr;
  let delay = initialDelayMs;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1) {
        await wait(delay);
        delay *= 2;
      }
    }
  }
  throw lastErr;
}

function nowIso() {
  return new Date().toISOString();
}

function makeRunId() {
  return crypto.randomBytes(6).toString("hex");
}

// Airtable helpers
const AIRTABLE_BASE = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;
const AIRTABLE_HEADERS = {
  Authorization: `Bearer ${AIRTABLE_TOKEN}`,
  "Content-Type": "application/json",
};

async function createAirtableRecord(fields) {
  const url = `${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}`;
  const resp = await fetch(url, {
    method: "POST",
    headers: AIRTABLE_HEADERS,
    body: JSON.stringify({ fields }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Airtable create failed: ${resp.status} ${text}`);
  }
  const data = await resp.json();
  return data;
}

async function updateAirtableRecord(recordId, fields) {
  const url = `${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}/${recordId}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: AIRTABLE_HEADERS,
    body: JSON.stringify({ fields }),
  });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Airtable update failed: ${resp.status} ${txt}`);
  }
  return resp.json();
}

async function getAirtableRecord(recordId) {
  const url = `${AIRTABLE_BASE}/${encodeURIComponent(AIRTABLE_TABLE)}/${recordId}`;
  const resp = await fetch(url, { headers: AIRTABLE_HEADERS });
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Airtable get failed: ${resp.status} ${txt}`);
  }
  return resp.json();
}

// Convert remote image URL to data URL (base64)
async function urlToDataUrl(imageUrl) {
  // Validate
  new URL(imageUrl); // will throw if invalid
  return await retryWithBackoff(
    async () => {
      const resp = await fetch(imageUrl);
      if (!resp.ok) throw new Error(`Failed fetch image ${imageUrl}: ${resp.status}`);
      const buf = Buffer.from(await resp.arrayBuffer());
      // Try to infer content-type from headers or extension
      let contentType = resp.headers.get("content-type") || undefined;
      if (!contentType) {
        // fallback by extension
        const ext = imageUrl.split(".").pop().toLowerCase();
        if (ext === "jpg" || ext === "jpeg") contentType = "image/jpeg";
        else if (ext === "png") contentType = "image/png";
        else if (ext === "webp") contentType = "image/webp";
        else contentType = "application/octet-stream";
      }
      return `data:${contentType};base64,${buf.toString("base64")}`;
    },
    3,
    400
  );
}

/**
 * WaveSpeed API helpers
 *
 * NOTE: Update endpoints/JSON shape to match your real WaveSpeed/Seedream v4 API.
 */

async function submitWaveSpeedJob({ prompt, subjectDataUrl, refDataUrls = [], width, height, webhookUrl }) {
  // Build payload expected by WaveSpeed Seedream v4. Adjust to API contract as needed.
  const endpoint = `${WAVESPEED_API_BASE}/seedream/generate`;
  const payload = {
    model: "seedream-v4",
    prompt,
    images: [subjectDataUrl, ...refDataUrls], // subject first per your requirement
    width,
    height,
    webhook: webhookUrl,
  };

  const res = await retryWithBackoff(
    async () => {
      const r = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${WAVESPEED_API_KEY}`,
        },
        body: JSON.stringify(payload),
      });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`WaveSpeed submit failed: ${r.status} ${t}`);
      }
      return r.json();
    },
    3,
    500
  );

  // Assume response contains { requestId: '...' } or similar. Adapt if needed.
  if (!res.requestId && !res.id) {
    throw new Error("WaveSpeed response missing requestId/id: " + JSON.stringify(res));
  }
  // Normalize
  return { requestId: res.requestId || res.id, raw: res };
}

async function fetchWaveSpeedResult(requestId) {
  const endpoint = `${WAVESPEED_API_BASE}/seedream/requests/${encodeURIComponent(requestId)}`;
  const r = await retryWithBackoff(
    async () => {
      const resp = await fetch(endpoint, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${WAVESPEED_API_KEY}`,
        },
      });
      if (!resp.ok) {
        const t = await resp.text();
        throw new Error(`WaveSpeed fetch failed: ${resp.status} ${t}`);
      }
      return resp.json();
    },
    3,
    500
  );
  return r; // Expect this to include status, outputs (maybe base64 or urls), etc.
}

/**
 * Helpers to manage parent row fields (strings join/split)
 */
function csvAdd(csvStr, value) {
  if (!csvStr || csvStr === "") return value;
  const arr = csvStr.split(",").map((s) => s.trim()).filter(Boolean);
  if (!arr.includes(value)) arr.push(value);
  return arr.join(",");
}

function csvAppendAll(csvStr, values = []) {
  const arr = (csvStr || "").split(",").map((s) => s.trim()).filter(Boolean);
  for (const v of values) if (!arr.includes(v)) arr.push(v);
  return arr.join(",");
}

/**
 * Main batch start handler
 */
app.post("/start-batch", express.urlencoded({ extended: true }), async (req, res) => {
  (async () => {
    try {
      const { prompt, subjectUrl, refUrls = "", width = "512", height = "512", batchCount = "1" } = req.body;
      const w = parseInt(width || "512", 10);
      const h = parseInt(height || "512", 10);
      const count = Math.max(1, Math.min(50, parseInt(batchCount || "1", 10)));

      const refList = refUrls
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);

      // Convert images to data URLs (subject first)
      const subjectDataUrl = await urlToDataUrl(subjectUrl);
      const refDataUrls = [];
      for (const ru of refList) {
        refDataUrls.push(await urlToDataUrl(ru));
      }

      // Create parent row in Airtable
      const runId = makeRunId();
      const initialFields = {
        Prompt: prompt,
        Subject: [{ url: subjectUrl }], // keep original URL as attachment as well
        References: refList.map((u) => ({ url: u })),
        Output: [], // attachments appended as jobs complete
        "Output URL": "",
        Model: "seedream-v4",
        Size: `${w}x${h}`,
        "Request IDs": "", // will populate as comma-separated
        "Seen IDs": "",
        "Failed IDs": "",
        Status: "processing",
        "Run ID": runId,
        "Created At": nowIso(),
        "Last Update": nowIso(),
      };

      const created = await createAirtableRecord(initialFields);
      const parentRecordId = created.id;
      console.log(`Created parent Airtable record ${parentRecordId} for run ${runId}`);

      // Start submitting jobs spaced by ~1.2s
      for (let i = 0; i < count; i++) {
        // delay between submissions to avoid API hiccups
        if (i > 0) await wait(1200);

        // submit one job
        try {
          const job = await submitWaveSpeedJob({
            prompt,
            subjectDataUrl,
            refDataUrls,
            width: w,
            height: h,
            webhookUrl: `${PUBLIC_BASE_URL}/webhooks/wavespeed`,
          });
          const requestId = job.requestId;
          console.log(`Submitted job ${i + 1}/${count}: ${requestId}`);

          // add Request ID to Airtable immediately
          await updateAirtableRecord(parentRecordId, {
            "Request IDs": csvAdd((created.fields?.["Request IDs"]) || "", requestId),
            "Last Update": nowIso(),
          });

          // Kick off polling in background (no await so multiple pollers run)
          pollUntilDone(requestId, parentRecordId).catch((err) => {
            console.error("pollUntilDone error:", err);
          });
        } catch (err) {
          console.error("Error submitting job:", err);
          // If submission fails, record this ID-less failure in Failed IDs by creating a placeholder token
          const failToken = `submit-failure-${Date.now()}-${i}`;
          await updateAirtableRecord(parentRecordId, {
            "Failed IDs": csvAdd((created.fields?.["Failed IDs"]) || "", failToken),
            "Last Update": nowIso(),
          });
        }
      }

      // Response to user: parent record created and batch started
      res.type("json").send({ ok: true, parentRecordId, runId, message: "Batch submitted. Running in background." });
    } catch (err) {
      console.error("start-batch error", err);
      try {
        res.status(500).json({ ok: false, error: err.message });
      } catch {}
    }
  })();
});

/**
 * Polling: pollUntilDone(requestId, parentId)
 * Polls every 7s (with retries), times out after ~20 minutes
 */
async function pollUntilDone(requestId, parentRecordId) {
  console.log(`pollUntilDone started for ${requestId}`);
  const pollIntervalMs = 7000;
  const timeoutMs = 20 * 60 * 1000; // 20 minutes
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetchWaveSpeedResult(requestId);
      // Expected shape: { status: 'processing'|'completed'|'failed', outputs: [ {url:..., base64:...} ], ... }
      const status = res.status || res.state || res.resultStatus;
      if (status === "completed" || status === "succeeded") {
        console.log(`Job ${requestId} completed (polled)`);
        await handleWaveSpeedResultPayload({ requestId, payload: res }, parentRecordId);
        return res;
      } else if (status === "failed" || status === "error") {
        console.log(`Job ${requestId} failed (polled)`);
        await markRequestFailed(parentRecordId, requestId, `polled-failure`);
        return res;
      } else {
        // still processing
        // optionally we can update Last Update in Airtable
        await updateAirtableRecord(parentRecordId, { "Last Update": nowIso() });
      }
    } catch (err) {
      console.warn(`Error polling ${requestId}: ${err.message}`);
    }
    await wait(pollIntervalMs);
  }

  // timed out
  console.warn(`Job ${requestId} timed out after ${timeoutMs / 1000} seconds`);
  await markRequestFailed(parentRecordId, requestId, "timeout");
  return { status: "timeout" };
}

/**
 * Mark a request as failed (append to Failed IDs and update Last Update)
 */
async function markRequestFailed(parentRecordId, requestId, reason = "") {
  try {
    const parent = await getAirtableRecord(parentRecordId);
    const currentFailed = (parent.fields?.["Failed IDs"]) || "";
    const currentLastUpdate = nowIso();
    const updatedFailed = csvAdd(currentFailed, requestId + (reason ? `:${reason}` : ""));
    // Also mark Seen IDs? no — not seen; leave Request ID in Request IDs.
    await updateAirtableRecord(parentRecordId, {
      "Failed IDs": updatedFailed,
      "Last Update": currentLastUpdate,
    });
    // After updating failed, check completion
    await tryFinalizeParent(parentRecordId);
  } catch (err) {
    console.error("markRequestFailed error:", err);
  }
}

/**
 * Append output attachments and mark Seen IDs when result arrives (either webhook or poll)
 * Expects payload that may contain outputs in one of multiple forms:
 * - payload.outputs: [{ url }] OR
 * - payload.outputs: [{ base64, filename }]
 * - Or payload may include direct image URLs
 */
async function handleWaveSpeedResultPayload({ requestId, payload }, parentRecordId) {
  try {
    // Pull parent row
    const parent = await getAirtableRecord(parentRecordId);
    const outputsField = parent.fields?.Output || [];
    const currentSeen = parent.fields?.["Seen IDs"] || "";
    const currentOutputUrls = parent.fields?.["Output URL"] || "";

    // Normalize outputs — try to extract known forms
    let attachmentsToAdd = []; // array of {url: ...} to add to Airtable
    // If payload has direct urls
    if (Array.isArray(payload.outputs) && payload.outputs.length > 0) {
      for (const o of payload.outputs) {
        if (o.url) {
          attachmentsToAdd.push({ url: o.url });
        } else if (o.base64) {
          // Attach base64 data URL
          const filename = o.filename || `${requestId}.png`;
          const dataUrl = o.base64.startsWith("data:") ? o.base64 : `data:image/png;base64,${o.base64}`;
          attachmentsToAdd.push({ url: dataUrl, filename });
        }
      }
    } else if (payload.result && Array.isArray(payload.result.images)) {
      for (const img of payload.result.images) {
        if (img.url) attachmentsToAdd.push({ url: img.url });
        else if (img.base64) attachmentsToAdd.push({ url: img.base64.startsWith("data:") ? img.base64 : `data:image/png;base64,${img.base64}` });
      }
    } else if (payload.image) {
      // single image
      const img = payload.image;
      if (img.url) attachmentsToAdd.push({ url: img.url });
      else if (img.base64) attachmentsToAdd.push({ url: img.base64.startsWith("data:") ? img.base64 : `data:image/png;base64,${img.base64}` });
    }

    // If no attachments could be extracted, but payload contains a URL field
    if (attachmentsToAdd.length === 0) {
      if (payload.outputUrl) attachmentsToAdd.push({ url: payload.outputUrl });
      else if (payload.url) attachmentsToAdd.push({ url: payload.url });
    }

    // Append new attachments to existing Output field for Airtable
    const newOutputs = Array.isArray(outputsField) ? outputsField.concat(attachmentsToAdd) : attachmentsToAdd;

    // Update Output and Seen IDs
    const updatedSeen = csvAdd(currentSeen, requestId);
    // If attachments include URLs, update Output URL to the last one (optional)
    let newOutputUrl = currentOutputUrls;
    if (attachmentsToAdd.length > 0) {
      // Try pick first http(s) url to expose in Output URL (not data:)
      const httpOne = attachmentsToAdd.find((a) => a.url && a.url.startsWith("http"));
      if (httpOne) newOutputUrl = httpOne.url;
    }

    await updateAirtableRecord(parentRecordId, {
      Output: newOutputs,
      "Seen IDs": updatedSeen,
      "Output URL": newOutputUrl,
      "Last Update": nowIso(),
    });

    // After updating, try finalize parent
    await tryFinalizeParent(parentRecordId);
  } catch (err) {
    console.error("handleWaveSpeedResultPayload error", err);
  }
}

/**
 * Check whether Seen IDs == Request IDs (set completed) or if all requests listed as failed/seen.
 * If so, mark Status completed and set Completed At.
 */
async function tryFinalizeParent(parentRecordId) {
  try {
    const parent = await getAirtableRecord(parentRecordId);
    const reqCsv = (parent.fields?.["Request IDs"] || "").split(",").map((s) => s.trim()).filter(Boolean);
    const seenCsv = (parent.fields?.["Seen IDs"] || "").split(",").map((s) => s.trim()).filter(Boolean);
    const failedCsv = (parent.fields?.["Failed IDs"] || "").split(",").map((s) => s.trim()).filter(Boolean);

    // If there are no Request IDs, treat as completed
    if (reqCsv.length === 0) {
      await updateAirtableRecord(parentRecordId, { Status: "completed", "Completed At": nowIso(), "Last Update": nowIso() });
      return;
    }

    // If every request id is either in seen OR in failed (matching base ids; note failed entries may contain :reason)
    const normalizedFailedIds = failedCsv.map((f) => f.split(":")[0]);
    const allHandled = reqCsv.every((rid) => seenCsv.includes(rid) || normalizedFailedIds.includes(rid));
    if (allHandled) {
      await updateAirtableRecord(parentRecordId, { Status: "completed", "Completed At": nowIso(), "Last Update": nowIso() });
      console.log(`Parent ${parentRecordId} marked completed`);
    } else {
      // still processing — update Last Update timestamp
      await updateAirtableRecord(parentRecordId, { "Last Update": nowIso() });
    }
  } catch (err) {
    console.error("tryFinalizeParent error", err);
  }
}

/**
 * Webhook handler t*
