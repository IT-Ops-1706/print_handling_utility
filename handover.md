# Handover — Flatten Orchestrator (`print_handling_utility`)

**Project:** PDF Flatten Orchestrator (middleware between ERP `PreAlertDoc` and the Print Queue API)
**Repository:** https://github.com/IT-Ops-1706/print_handling_utility
**Handover date:** 2026-05-05

This document is the operator-facing handover for the `print_handling_utility`. Read alongside `setup.md` (install steps) and `business_logic.md` (what the code does and why).

---

## 1. What This Service Does (90-Second Version)

Every 15 minutes it asks the ERP for new customs / shipping documents (BOE, OOC, eGatepass, Shipping Bill / LEO / eGatepass), sends them to the local Print Queue API for Adobe Acrobat flattening, then posts the flattened PDF back to the ERP under a paired "flatten" docType. Multi-page PDFs are optimised — only page 1 (the signature page) goes through Acrobat; pages 2..N are merged back in afterwards.

State lives entirely on disk under `States/`. There is no database.

---

## 2. Repository / Branch State at Handover

- **Remote:** `origin = https://github.com/IT-Ops-1706/print_handling_utility.git`
- **Branch:** `master`
- **Commits at handover (newest first):**
  - *(this handover commit — adds `setup.md`, `business_logic.md`, `handover.md`; updates `models.py`, `orchestrator.py`, `requirements.txt`)*
  - `c078a9a` — commit - 4/10
  - `fea06f9` — added UI
  - `4e7ae93` — Initial clean commit (no secrets)

---

## 3. Critical Operating Requirements

These are the things that *must* be true on the host or the service silently fails / produces garbage output.

### 3.1 Adobe Acrobat must be the default PDF opener

The Print Queue invokes the OS-level "open PDF" handler. If anything other than Adobe Acrobat (Edge, Chrome PDF viewer, SumatraPDF, Foxit, etc.) is the default, flattening targets the wrong app and either fails or produces a non-flattened file.

To set / verify:

1. Right-click any `.pdf` -> **Open with -> Choose another app**.
2. Pick **Adobe Acrobat** (or **Adobe Acrobat Reader DC**) and tick **Always use this app to open .pdf files**.
3. Confirm the registry value:
   ```powershell
   Get-ItemProperty HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\FileExts\.pdf\UserChoice
   ```
   `ProgId` should reference `Acrobat` / `AcroExch.Document.DC`.

### 3.2 Adobe popups must be removed / suppressed

UI automation cannot dismiss arbitrary modal dialogs, so every Adobe popup that appears in front of the print dialog will block or break a job. Open Acrobat once manually and turn the following **off**:

- **Edit -> Preferences -> General** -> *Show me messages when I launch Acrobat* — **off**.
- **Edit -> Preferences -> General** -> *Show online storage when opening / saving files* — **off** (both checkboxes).
- **Edit -> Preferences -> General** -> click **Select Default PDF Handler** and confirm this Acrobat install.
- **Edit -> Preferences -> Security (Enhanced)** -> *Enable Protected Mode at startup* and *Enable Enhanced Security* — **off**. Restart Acrobat.
- **Edit -> Preferences -> Trust Manager** -> set PDF/A view mode to *Never* and disable internet-access prompts.
- **Edit -> Preferences -> Updater** -> *Do not download or install updates automatically*.
- **Sign in / Adobe Cloud** panel -> **Sign out**; close any "Continue with Adobe" / "What's new" overlay.
- Right-side **Tools / All Tools** pane -> collapse / hide so the print dialog has predictable focus.
- Open a sample PDF and confirm it opens straight to the document with no welcome screen, no sign-in prompt, no update prompt; `Ctrl+P` opens the Print dialog immediately; closing the file does not prompt to save.

If a new popup ever appears (typically after an Adobe auto-update reverts a setting), the service will start failing with `error_type=ui_automation_failed` or `timeout`. First diagnostic step is always: open Acrobat by hand and look for a popup.

### 3.3 The user account must have an interactive desktop session

Adobe's UI automation requires a real Windows desktop. Run the orchestrator (and the Print Queue) as a logged-in user, or under NSSM with a service account that has an active session. Running under `LocalSystem` will not work.

### 3.4 The Print Queue and the ERP must be reachable

Before starting the orchestrator, confirm:

```powershell
curl http://127.0.0.1:8001/                 # Print Queue health
curl http://localhost:5056/api/Login -X POST ...  # ERP reachability
```

Both default URLs are configurable via `.env`.

---

## 4. Configuration at a Glance

All configuration lives in `.env` (loaded by `config.py`). The committed `.env.example` is the template — never commit a populated `.env`.

| Key | Default | Notes |
|---|---|---|
| `ERP_BASE_URL` | `http://localhost:5056` | ERP `PreAlertDoc` host |
| `ERP_EMAIL` | `it.ops@babajishivram.com` | ERP login user |
| `ERP_PASSWORD` | *(unset)* | Required |
| `PRINT_QUEUE_URL` | `http://127.0.0.1:8001` | Print Queue API |
| `PRINT_API_KEY` | *(unset)* | Bearer token for Print Queue |
| `FETCH_INTERVAL_MINUTES` | `15` | Cycle period |
| `POLL_INTERVAL_SECONDS` | `30` | Print Queue poll cadence |
| `MAX_POLL_TIMEOUT_SECONDS` | `90` | Per-job poll timeout |
| `BATCH_SIZE` | `10` | Documents per batch |
| `MAX_RETRIES` | `3` | Per-document retry budget |

---

## 5. Day-to-Day Operations

### 5.1 Start / Stop

- **Start:** activate the venv, `python main.py`. Or start the configured Windows service (NSSM / Task Scheduler).
- **Stop:** `Ctrl+C` in the console, or stop the service. Shutdown is clean — any job in `polling` becomes `pending_retry` on the next start.
- **Restart safety:** the orchestrator is designed to be restarted at any time. `States/active_jobs.json` plus `processed_lids.json` ensure no document is processed twice and no in-flight job is lost.

### 5.2 Where to look for output

| Location | Contents |
|---|---|
| `logs/orchestrator.log` | Daily-rotated full log (DEBUG level) — the source of truth for diagnostics |
| Console | INFO and above — useful for a live view |
| `States/active_jobs.json` | Currently in-flight + pending-retry jobs |
| `States/processed_lids.json` | Every `lId` ever processed (dedup ledger) |
| `States/Mon_YYYY/DD_MM/completed.json` | Today's successes |
| `States/Mon_YYYY/DD_MM/failed.json` | Today's permanent failures |
| `cache/` | Working PDFs — empty in steady state, populated only for in-flight or failed jobs |

### 5.3 Routine checks

- Tail `logs/orchestrator.log` and confirm a new "CYCLE START / CYCLE COMPLETE" pair every `FETCH_INTERVAL_MINUTES`.
- Spot-check today's `failed.json` — anything unexpected should be triaged the same day.
- Spot-check `cache/` — files older than a few hours indicate a stuck or permanently failed job that was kept for inspection.

---

## 6. Common Failure Modes and Triage

| Symptom | Likely cause | First action |
|---|---|---|
| `error_type=acrobat_not_found` | Adobe Acrobat not installed, not the default PDF handler, or path changed after an update | Re-run section 3.1 of this doc; restart Acrobat |
| `error_type=ui_automation_failed` or repeated `timeout` | A new Adobe popup is blocking the print dialog (often after an Acrobat auto-update) | Open a PDF manually in Acrobat; dismiss any popup; re-run section 3.2 |
| `error_type=print_queue_unavailable` | Print Queue process down or wrong `PRINT_QUEUE_URL` | `curl` the URL; restart the Print Queue service |
| `error_type=file_locked` | Antivirus / Search Indexer holding the cached PDF open | Usually retried automatically; if persistent, exclude `cache/` from antivirus and Windows Search |
| ERP saves return non-200 | Token expired (one-shot reauth handled), or ERP rejected the payload | Check `logs/orchestrator.log` for the response body; verify ERP credentials |
| `process_crash` jobs on startup | Previous run was killed mid-poll | Expected; they roll into `pending_retry` automatically |

For any permanently failed job, the cached `input_*` / `firstpage_*` files in `cache/` are kept — drag one into Acrobat manually to reproduce the failure.

### 6.1 Manual replay

To re-run a specific `lId`:
1. Stop the service.
2. Open `States/processed_lids.json` and remove the offending `lId`.
3. Open the relevant `States/Mon_YYYY/DD_MM/failed.json` and remove the matching `doc_<lId>` entry.
4. Restart the service. The next fetch cycle will pick the document up again (assuming `last_fetch_timestamp` is older than the document's ERP timestamp; if not, also rewind `metadata.json::last_fetch_timestamp`).

---

## 7. What Changed in This Handover

- Added top-level documentation: `setup.md`, `business_logic.md`, `handover.md`.
- Updated `models.py`, `orchestrator.py`, `requirements.txt` (uncommitted local edits brought into version control as part of this handover commit).
- **Operational pre-requisite documented** — Adobe Acrobat must be set as the default PDF opener (section 3.1) and **all Adobe popups must be disabled** so the Print Queue's UI automation has a clean print path (section 3.2). These environment requirements are not enforced by code; they must be configured on the host.

The existing `docs/SRS.md` (the original Software Requirements Specification, dated 2026-02-19) remains the canonical functional spec and is unchanged by this handover.

---

## 8. Out-of-Scope / Known Boundaries

The orchestrator deliberately does not handle:

- **Adobe automation itself** — that is the `pdf_printing` (Print Queue API) project.
- **Document merging at the business level** — handled by the separate PDF Merge Utility.
- **Digital signing** — handled by the Digital Sign utility.
- **Notifications / email** — `check_email_token.py` exists for verifying Microsoft Graph `Mail.Send` permission for an unrelated email path; it is not wired into the orchestrator's main loop.

Anything in those areas should be raised against the appropriate sibling project, not patched into this one.

---

## 9. Contacts and Ownership

- **Code owner:** IT Ops (`it.ops@babajishivram.com`)
- **GitHub:** https://github.com/IT-Ops-1706/print_handling_utility
- **Companion projects (same parent folder):** `pdf_printing` (Print Queue), `pdf_merge_utility`, `pdf_digital_sign`

For new features, open an issue or PR on the GitHub repo. For production incidents, restart the service first (it is safe), then triage using sections 5 and 6.
