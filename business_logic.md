# Business Logic — Flatten Orchestrator

This document describes the end-to-end business logic of the `print_handling_utility` (Flatten Orchestrator) — *what* it does, *why*, and *how* the pieces fit together.

---

## 1. Why This Utility Exists

Documents downloaded from Indian Customs (ICEGATE) and similar government portals carry a **live digital signature** field. When those PDFs are merged with other pages and forwarded to end users, most viewers display *"Signature Not Verified"* with a yellow question mark instead of the expected green tick.

Adobe Acrobat's *Print to PDF* operation rasterises the signature into static content — this is what we call **flattening**. After flattening, every viewer renders the page as a normal, valid-looking signed page.

The Flatten Orchestrator is the middleware that ties two existing systems together:

- **ERP API (`PreAlertDoc`)** — source of unflattened documents and destination of flattened ones.
- **Print Queue API (`pdf_printing` project)** — performs the actual Adobe Acrobat print-to-PDF flattening.

The orchestrator does not flatten anything itself. It fetches, dispatches, polls, merges, and saves.

---

## 2. High-Level Pipeline

```
              every FETCH_INTERVAL_MINUTES (default 15 min)
                              |
                              v
     +------------------------------------------------+
     |                CYCLE                           |
     |                                                |
     |  Phase 1 — RETRY                               |
     |    For every active job in 'pending_retry'     |
     |    re-run the submit/poll/save flow.           |
     |                                                |
     |  Phase 2 — FETCH                               |
     |    GET /PreAlertDoc/GetDocsByDate              |
     |    Filter to supported docTypes.               |
     |    Skip any lId already tracked or processed.  |
     |    For multi-page PDFs, extract page 1.        |
     |                                                |
     |  Phase 3 — PROCESS (batched, sliding window)   |
     |    Submit -> poll -> merge -> save to ERP.     |
     |    Cleanup cached files only on ERP success.   |
     |                                                |
     |  Update last_fetch_timestamp                   |
     +------------------------------------------------+
```

Implemented in `orchestrator.py::Orchestrator.run_cycle`. Driven by the `while True:` loop in `main.py::main`.

---

## 3. Document Type Mapping

Only six document types are processed. Each has a separate "flatten" docType that the ERP uses to store the flattened copy alongside the original.

| Source docType (from ERP) | Flatten docType (back to ERP) | Document |
|---|---|---|
| 104 | 126 | BOE Copy Flatten |
| 109 | 127 | Final OOC Copy Flatten |
| 110 | 128 | eGatepass Copy Flatten |
| 111 | 129 | Shipping Bill Copy Flatten |
| 112 | 130 | Shipping Final LEO Copy Flatten |
| 113 | 131 | Shipping eGatepass Copy Flatten |

Source: `config.py::Config.DOC_TYPE_MAP`. Anything outside this set is logged and skipped during the fetch phase.

---

## 4. Phase 1 — Retry

Driver: `orchestrator.py::Orchestrator._retry_phase`.

- Reads `States/active_jobs.json` and selects every job whose `status == "pending_retry"`.
- Feeds them through the same sliding-window processor used for fresh documents (Phase 3) — so a retry is just a re-submit of the same cached file.
- Cached `input_*` / `firstpage_*` files were intentionally **kept** on the previous failure so retry can re-use them without re-downloading from the ERP.
- On startup, any job whose status was `polling` is reset to `pending_retry` with `error_type=process_crash` (see `state_manager.py::StateManager._load`). This makes restarts safe.

A document is eligible for auto-retry only when its `error_type` is in `RETRYABLE_ERRORS` (`timeout`, `acrobat_not_found`, `file_locked`, `print_queue_unavailable`). Anything else flips straight to `permanently_failed`.

---

## 5. Phase 2 — Fetch

Driver: `orchestrator.py::Orchestrator._fetch_phase`.

1. Read `last_fetch_timestamp` from `States/metadata.json`. If absent, fall back to a hard-coded bootstrap timestamp (`2026-02-16T13:09:01.300`).
2. `GET /api/PreAlertDoc/GetDocsByDate?fromDate={timestamp}` via `ERPClient.fetch_documents`.
3. For each returned document:
   - **Dedup** — skip if `lId` is already in `active_jobs` or in `processed_lids.json`.
   - **Type filter** — drop documents whose `docType` is not in `DOC_TYPE_MAP`.
   - **Content filter** — drop documents whose `fileBase64` is empty/blank.
   - **Cache the input** — decode base64 to `cache/input_{lId}_{filename}`.
   - **First-page optimisation** — `_extract_first_page` reads the PDF; if it has more than one page, only page 1 is saved as `cache/firstpage_{lId}_{filename}` and `needs_merge=True` is recorded.
4. Materialise a `TrackedJob` per document and persist it to `active_jobs.json`.

The `last_fetch_timestamp` is **not** updated here — it is updated only at the very end of the cycle, so a crash mid-cycle is safe (the next run re-fetches the same window).

### 5.1 First-Page Optimisation — Why?

Adobe Acrobat's print automation is the slow, fragile part of the system. Only page 1 carries the digital signature, so feeding the Print Queue a single page instead of (often) 30+ pages cuts flattening time dramatically and reduces the chance of UI automation failure. The non-signature pages are merged back in after flattening (see Section 7.2).

---

## 6. Phase 3 — Process (Batched Sliding Window)

Driver: `orchestrator.py::Orchestrator._process_jobs_sliding_window`.

The Print Queue has **one** Adobe Acrobat worker. Naively submitting and waiting per-job means the worker sits idle while we poll. Naively submitting many jobs up front means polling timeouts fire while the job is still queued.

The solution is a **depth-2 sliding window**:

1. Take a batch of up to `BATCH_SIZE` (default 10) jobs.
2. Submit the first 2 jobs back-to-back.
3. Poll the **oldest** submitted job until it resolves (`completed` / `failed` / `timeout`).
4. As soon as that job resolves, submit the next pending job — restoring queue depth of 2.
5. Handle the result (save to ERP on success, retry/fail on error).
6. Repeat until the batch is empty.

Because the timeout clock for job *N+1* only starts when job *N* finishes, queued jobs never trigger false timeouts. Because the queue always has one job ready behind the active one, the Adobe worker never idles between jobs.

---

## 7. Submit / Poll / Save Flow

For each job inside the sliding window:

### 7.1 Submit

`print_queue_client.py::PrintQueueClient.submit_job` — `POST /print-queue` as `multipart/form-data` with the cached PDF (first page if `needs_merge`, otherwise the full file). On success the response carries a `job_id` which is stored on the `TrackedJob` and persisted.

Failure modes are mapped to `error_type`s that drive retry decisions:
- `print_queue_unavailable` (connection / request errors) — retryable.
- `file_not_found` — non-retryable (cache was deleted out from under us).
- `unknown` — non-retryable HTTP errors.

### 7.2 Poll

`print_queue_client.py::PrintQueueClient.poll_until_complete` — `GET /job-status/{job_id}` every `POLL_INTERVAL_SECONDS` (default 30s) up to `MAX_POLL_TIMEOUT_SECONDS` (default 90s). The timeout clock starts the moment polling begins, *not* when the job was submitted.

On `completed`, the response includes `result` (base64 of the flattened PDF). On `failed`, it carries the `error` and `error_type` reported by the Print Queue itself (e.g. `acrobat_not_found`, `file_locked`, `timeout`, `ui_automation_failed`).

### 7.3 Merge (multi-page only)

`orchestrator.py::Orchestrator._merge_flattened_first_page` — when `needs_merge` is true, the flattened first page is recombined with pages 2..N of the *original* cached PDF using `pypdf.PdfMerger`. The merge happens entirely in memory (`io.BytesIO`) — no extra temp files.

### 7.4 Save to ERP

`erp_client.py::ERPClient.save_flatten_doc` — `POST /api/PreAlertDoc/SaveFlattenDoc` with:

```json
{
  "lId": <original lId>,
  "jobId": <original jobId>,
  "docPath": "<original docPath>",
  "fileName": "<original_basename>_flatten.pdf",
  "docType": <flatten docType from DOC_TYPE_MAP>,
  "flattenQueueId": "<print queue job_id>",
  "flattenStatusId": 2,
  "fileBase64": "<flattened pdf base64>"
}
```

`flattenStatusId` is hard-coded to `2` for successful saves (the constant the ERP currently expects for "flatten complete").

### 7.5 Cleanup

Cached files (`input_*`, `firstpage_*`, `flatten_*`) are deleted **only after the ERP returns 200/201**. On any failure the files are left on disk so an operator can inspect or re-run them manually.

The completed job record is moved out of `active_jobs.json` into the date-bucketed `States/Mon_YYYY/DD_MM/completed.json`, and the `lId` is appended to `processed_lids.json` so it cannot be picked up again.

---

## 8. Error Handling and Retry Policy

`orchestrator.py::Orchestrator._handle_failure`.

| Condition | Outcome |
|---|---|
| `error_type ∈ RETRYABLE_ERRORS` and `retry_count + 1 < MAX_RETRIES` | Status set to `pending_retry`. Cached files kept. Picked up by Phase 1 next cycle. |
| `error_type ∉ RETRYABLE_ERRORS` | Permanently failed. Moved to date-bucketed `failed.json`. |
| Retry budget exceeded | Permanently failed. Moved to date-bucketed `failed.json`. |

`MAX_RETRIES` is 3 by default. On permanent failure the cached PDFs are intentionally **not** deleted — operators rely on them for diagnosis and manual replay.

Authentication failures with the ERP are handled transparently in `ERPClient._request_with_reauth`: a single 401 triggers a re-login and one retry of the same request.

---

## 9. State Persistence

`state_manager.py` owns all on-disk state. Layout under `States/`:

| File | Contents |
|---|---|
| `metadata.json` | `last_fetch_timestamp` only |
| `active_jobs.json` | Operational set: jobs in `fetched / submitted / polling / pending_retry`. Loaded into memory at startup. |
| `processed_lids.json` | Append-only sorted list of every `lId` that has ever reached `completed` or `permanently_failed`. Drives dedup. |
| `Mon_YYYY/DD_MM/completed.json` | Completed jobs for that calendar day |
| `Mon_YYYY/DD_MM/failed.json` | Permanently failed jobs for that calendar day |

All writes are **atomic** — temp file in the same directory followed by `os.replace()`, with up to 5 retries on Windows `PermissionError` (WinError 5) caused by transient locks from antivirus / Search Indexer / the UI. See `state_manager.py::StateManager._write_json_atomic`.

Crash recovery: on load, any job whose status was `polling` is rewritten as `pending_retry` with `error_type=process_crash` so the next cycle picks it up.

---

## 10. Scheduling and Lifetime

Implemented in `main.py`:

- `setup_logging()` — console handler at INFO and a daily-rotated `logs/orchestrator.log` file handler at DEBUG.
- `main()` — instantiate `Orchestrator`, then loop forever:
  1. Run `orchestrator.run_cycle()`.
  2. Log the per-cycle summary (`fetched / submitted / completed / saved / failed / retried`).
  3. `time.sleep(FETCH_INTERVAL_MINUTES * 60)`.
- `KeyboardInterrupt` exits cleanly.

Unhandled exceptions inside a cycle are logged with a stack trace; the loop continues to the next cycle. The orchestrator is designed to run continuously as a Windows service (NSSM or Task Scheduler).

---

## 11. Boundaries and Non-Goals

The Flatten Orchestrator deliberately does **not**:

- Perform Adobe Acrobat automation (handled by the Print Queue / `pdf_printing` project).
- Merge or split documents at a business level (handled by the PDF Merge Utility).
- Apply digital signatures (handled by the Digital Sign Utility).
- Talk to any external network — every endpoint it touches is on the same LAN as the ERP, the Print Queue, and the Adobe Acrobat host.

Everything in this document is implemented in roughly 700 lines of Python across `main.py`, `config.py`, `orchestrator.py`, `erp_client.py`, `print_queue_client.py`, `state_manager.py`, and `models.py`. There is no database — `States/*.json` is the source of truth.
