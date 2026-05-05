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
- **Latest commits at handover:** documentation commit (`setup.md`, `business_logic.md`, `handover.md`) plus the synced edits to `models.py`, `orchestrator.py`, `requirements.txt`.

---

## 3. Critical Operating Requirements

These are the things that must be true on the host for the service to run correctly.

### 3.1 Adobe Acrobat must be the default PDF opener

The Print Queue invokes the OS-level "open PDF" handler. Adobe Acrobat must be set as the default for `.pdf`.

To set / verify:

1. Right-click any `.pdf` -> **Open with -> Choose another app**.
2. Pick **Adobe Acrobat** (or **Adobe Acrobat Reader DC**) and tick **Always use this app to open .pdf files**.
3. Confirm the registry value:
   ```powershell
   Get-ItemProperty HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\FileExts\.pdf\UserChoice
   ```
   `ProgId` should reference `Acrobat` / `AcroExch.Document.DC`.

### 3.2 Adobe popups must be removed / suppressed

Open Acrobat once manually and turn the following off so no modal dialog blocks the print path:

- **Edit -> Preferences -> General** -> *Show me messages when I launch Acrobat* — **off**.
- **Edit -> Preferences -> General** -> *Show online storage when opening / saving files* — **off** (both checkboxes).
- **Edit -> Preferences -> General** -> click **Select Default PDF Handler** and confirm this Acrobat install.
- **Edit -> Preferences -> Security (Enhanced)** -> *Enable Protected Mode at startup* and *Enable Enhanced Security* — **off**. Restart Acrobat.
- **Edit -> Preferences -> Trust Manager** -> set PDF/A view mode to *Never* and disable internet-access prompts.
- **Edit -> Preferences -> Updater** -> *Do not download or install updates automatically*.
- **Sign in / Adobe Cloud** panel -> **Sign out**; close any "Continue with Adobe" / "What's new" overlay.
- Right-side **Tools / All Tools** pane -> collapse / hide so the print dialog has predictable focus.
- Open a sample PDF and confirm: opens straight to the document with no welcome screen, no sign-in prompt, no update prompt; `Ctrl+P` opens the Print dialog immediately; closing the file does not prompt to save.

### 3.3 Interactive desktop session

Adobe's UI automation requires a real Windows desktop. Run the orchestrator (and the Print Queue) under a logged-in user account, or under NSSM with a service account that has an active session.

### 3.4 Upstream services reachable

The Print Queue and the ERP must be reachable on the configured URLs:

```powershell
curl http://127.0.0.1:8001/                       # Print Queue health
curl http://localhost:5056/api/Login -X POST ...  # ERP reachability
```

Both default URLs are configurable via `.env`.

---

## 4. Configuration at a Glance

All configuration lives in `.env` (loaded by `config.py`). The committed `.env.example` is the template — `.env` itself is gitignored.

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
| `logs/orchestrator.log` | Daily-rotated full log (DEBUG level) |
| Console | INFO and above |
| `States/active_jobs.json` | In-flight + pending-retry jobs |
| `States/processed_lids.json` | Every `lId` ever processed (dedup ledger) |
| `States/Mon_YYYY/DD_MM/completed.json` | Today's successes |
| `States/Mon_YYYY/DD_MM/failed.json` | Today's permanent failures |
| `cache/` | Working PDFs — empty in steady state |

A healthy run produces a "CYCLE START" / "CYCLE COMPLETE" pair in `orchestrator.log` every `FETCH_INTERVAL_MINUTES`.

---

## 6. Out-of-Scope / Boundaries

The Flatten Orchestrator deliberately does not handle:

- **Adobe automation itself** — that is the `pdf_printing` (Print Queue API) project.
- **Document merging at the business level** — handled by the `pdf_merge_utility` project.
- **Digital signing** — handled by the `pdf_digital_sign` project.
- **Notifications / email** — `check_email_token.py` is a standalone helper for verifying Microsoft Graph `Mail.Send` permission and is not wired into the orchestrator's main loop.

---

## 7. Ownership

- **Code owner:** IT Ops (`it.ops@babajishivram.com`)
- **GitHub:** https://github.com/IT-Ops-1706/print_handling_utility
- **Companion projects (same parent folder):** `pdf_printing` (Print Queue), `pdf_merge_utility`, `pdf_digital_sign`
