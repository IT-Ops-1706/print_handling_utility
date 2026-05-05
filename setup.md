# Setup Guide — Flatten Orchestrator (`print_handling_utility`)

This document describes how to set up the Flatten Orchestrator on a fresh Windows machine.

---

## 1. Prerequisites

| Requirement | Version / Notes |
|---|---|
| OS | Windows 10 / 11 (the orchestrator is designed for the same host that runs the Print Queue API and Adobe Acrobat) |
| Python | 3.10 or higher (3.11+ recommended — uses `dict[str, ...]` and `str \| None` syntax) |
| Adobe Acrobat | Adobe Acrobat (Pro / Reader DC) installed and configured as described in section 5 |
| Network access | Reachability to the ERP host (default `http://localhost:5056`) and the Print Queue API (default `http://127.0.0.1:8001`) |
| Git | Required only if cloning from GitHub |

The companion **Print Queue API** (`pdf_printing` project) and the **ERP API** (PreAlertDoc) must be running and reachable before the orchestrator is started.

---

## 2. Get the Code

```powershell
git clone https://github.com/IT-Ops-1706/print_handling_utility.git
cd print_handling_utility
```

Or copy the existing project folder onto the target machine.

---

## 3. Create a Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation script:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

---

## 4. Install Dependencies

```powershell
pip install -r requirements.txt
```

Installed packages:

- `httpx` — async-friendly HTTP client used for Print Queue calls
- `requests` + `urllib3` — HTTP client used for ERP calls (with custom SSL adapter)
- `pydantic` — data models for API contracts
- `pypdf>=3.17.0` — first-page extraction and merge of flattened pages
- `python-dotenv` — `.env` loader for configuration

---

## 5. Adobe Acrobat Configuration (Critical)

The Print Queue depends on Adobe Acrobat being the default PDF handler and on **all** Adobe popups/dialogs being suppressed. Without this, the print automation either targets the wrong app or stalls on a modal dialog.

### 5.1 Set Adobe Acrobat as the default PDF opener

1. Right-click any `.pdf` file -> **Open with -> Choose another app**.
2. Select **Adobe Acrobat** (or **Adobe Acrobat Reader DC**).
3. Tick **Always use this app to open .pdf files** -> click **OK**.
4. Verify with `Get-ItemProperty HKCU:\Software\Microsoft\Windows\CurrentVersion\Explorer\FileExts\.pdf\UserChoice` — the `ProgId` should reference `Acrobat` / `AcroExch.Document.DC`.

### 5.2 Remove all Adobe popups so they do not block automation

Open Adobe Acrobat once manually, dismiss any first-run prompts, then in **Edit -> Preferences**:

- **General** -> uncheck *Show me messages when I launch Acrobat*.
- **General** -> uncheck *Show online storage when opening files* / *Show online storage when saving files*.
- **General** -> set *Default PDF Handler* to this Adobe install (click **Select Default PDF Handler** if shown).
- **Documents** -> uncheck *Show all documents in Recent list of File menu* (optional, reduces UI noise).
- **Security (Enhanced)** -> uncheck *Enable Protected Mode at startup* and *Enable Enhanced Security*. (Restart Acrobat afterwards.)
- **Trust Manager** -> set PDF/A view mode and trusted-domain prompts to *Never*.
- Sign in / Adobe Cloud panel -> **Sign out** of any Adobe ID; close the "Continue with Adobe" banner.
- Dismiss the *Tools / All Tools* pane (right side panel) so the print dialog has predictable focus.
- Updater popup -> **Edit -> Preferences -> Updater** -> set to *Do not download or install updates automatically*.

After these changes, double-click a sample PDF and confirm that:

- It opens directly in Adobe Acrobat with no welcome / sign-in / "what's new" overlay.
- `Ctrl+P` opens the print dialog immediately with no intervening prompts.
- Closing the document does not prompt to save.

If any popup still appears, capture its title and re-run the relevant Acrobat preference toggle. The Print Queue's UI automation assumes a clean, popup-free Acrobat.

---

## 6. Configure Environment Variables

Copy the template and fill it in:

```powershell
copy .env.example .env
notepad .env
```

Required keys (see `.env.example` for the full list):

| Variable | Purpose |
|---|---|
| `ERP_BASE_URL` | Base URL of the ERP API (e.g. `http://localhost:5056`) |
| `ERP_EMAIL` | ERP login email (default in code: `it.ops@babajishivram.com`) |
| `ERP_PASSWORD` | ERP login password (no default — must be set) |
| `PRINT_QUEUE_URL` | Print Queue API base URL (default `http://127.0.0.1:8001`) |
| `PRINT_API_KEY` | Bearer token expected by the Print Queue API |
| `FETCH_INTERVAL_MINUTES` | Cycle interval (default `15`) |
| `POLL_INTERVAL_SECONDS` | Print Queue poll interval (default `30`) |
| `MAX_POLL_TIMEOUT_SECONDS` | Per-job poll timeout (default `90`) |
| `BATCH_SIZE` | Documents per processing batch (default `10`) |
| `MAX_RETRIES` | Retry attempts per document (default `3`) |

`config.py` reads these via `python-dotenv`. Never commit a populated `.env` — it is excluded by `.gitignore`.

---

## 7. Verify Connectivity

Before starting the orchestrator, sanity-check both upstreams:

```powershell
# Print Queue API health
curl http://127.0.0.1:8001/

# ERP login (replace with real credentials)
curl -X POST http://localhost:5056/api/Login -H "Content-Type: application/json" -d "{\"email\":\"it.ops@babajishivram.com\",\"password\":\"...\",\"moduleId\":0,\"branchId\":0}"
```

If `check_email_token.py` is needed for the Azure email path, run it once to confirm the Microsoft Graph token has the required `Mail.Send` permission.

---

## 8. Run the Orchestrator

```powershell
python main.py
```

Expected console output on startup:

```
============================================================
FLATTEN ORCHESTRATOR STARTING
Fetch interval: 15 minutes
Poll interval: 30 seconds
Poll timeout: 90 seconds
Batch size: 10
Max retries: 3
Retryable errors: ['timeout', 'acrobat_not_found', 'file_locked', 'print_queue_unavailable']
Supported doc types: [104, 109, 110, 111, 112, 113]
============================================================
```

The process runs forever. Stop it with `Ctrl+C`.

---

## 9. Generated Directories

The first run will auto-create:

| Directory | Purpose |
|---|---|
| `logs/` | Daily-rotated `orchestrator.log` (DEBUG to file, INFO to console) |
| `cache/` | Working PDF files (`input_*`, `firstpage_*`, `flatten_*`) — deleted on successful ERP save, kept on failure |
| `States/` | Persistent state: `metadata.json`, `active_jobs.json`, `processed_lids.json`, plus `Mon_YYYY/DD_MM/` date-wise `completed.json` / `failed.json` |

These three folders are intentionally excluded from version control (see `.gitignore`).

---

## 10. Run as a Background Service (Optional)

For production, run the orchestrator under a process supervisor so it restarts on reboot:

- **NSSM** — `nssm install FlattenOrchestrator "C:\path\to\venv\Scripts\python.exe" "C:\path\to\print_handling_utility\main.py"`
- **Task Scheduler** — create a task that runs `main.py` at user logon and restarts on failure.

In either case, ensure the service runs as a user that has an active Windows desktop session — Adobe Acrobat's UI automation requires it.
