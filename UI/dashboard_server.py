"""
Dashboard API server for the Flatten Orchestrator.
Reads state files and logs to serve a monitoring dashboard.
"""
import os
import re
import json
import glob
import logging
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent
STATES_DIR = BASE_DIR / "States"
LOGS_DIR = BASE_DIR / "logs"
UI_DIR = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)

app = FastAPI(title="Flatten Orchestrator Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _read_json(path: str) -> dict | list | None:
    """Read a JSON file, return None on error."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _parse_date_folder(month_folder: str, day_folder: str) -> datetime | None:
    """Parse 'Feb_2026' + '20_02' into a datetime."""
    try:
        # day_folder is DD_MM, month_folder is Mon_YYYY
        day, month = day_folder.split("_")
        _, year = month_folder.split("_")
        return datetime(int(year), int(month), int(day))
    except Exception:
        return None


def _collect_all_jobs(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    status_filter: str | None = None,
) -> list[dict]:
    """Walk States directory and collect all completed + failed jobs, plus active ones."""
    all_jobs = []

    # --- Date-wise completed/failed files ---
    if STATES_DIR.exists():
        for month_dir in sorted(STATES_DIR.iterdir()):
            if not month_dir.is_dir() or month_dir.name.startswith("."):
                continue
            # Skip non-month folders (metadata files, etc.)
            if "_" not in month_dir.name:
                continue
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir():
                    continue

                folder_date = _parse_date_folder(month_dir.name, day_dir.name)
                if not folder_date:
                    continue

                # Date filter
                if from_date and folder_date.date() < from_date.date():
                    continue
                if to_date and folder_date.date() > to_date.date():
                    continue

                # Read completed.json
                completed = _read_json(str(day_dir / "completed.json")) or {}
                for key, record in completed.items():
                    record["_source"] = "completed"
                    record.setdefault("status", "saved_to_erp")
                    record["_date"] = folder_date.isoformat()
                    if not status_filter or status_filter == "saved_to_erp" or status_filter == "completed":
                        all_jobs.append(record)

                # Read failed.json
                failed = _read_json(str(day_dir / "failed.json")) or {}
                for key, record in failed.items():
                    record["_source"] = "failed"
                    record.setdefault("status", "permanently_failed")
                    record["_date"] = folder_date.isoformat()
                    if not status_filter or status_filter == "permanently_failed" or status_filter == "failed":
                        all_jobs.append(record)

    # --- Active jobs ---
    active_file = STATES_DIR / "active_jobs.json"
    active_raw = _read_json(str(active_file)) or {}
    for key, record in active_raw.items():
        record["_source"] = "active"
        record["_date"] = record.get("submitted_at", datetime.now().isoformat())
        if not status_filter or status_filter == record.get("status", ""):
            # Apply date filter to active jobs by submitted_at
            if from_date or to_date:
                try:
                    job_date = datetime.fromisoformat(record.get("submitted_at", ""))
                    if from_date and job_date.date() < from_date.date():
                        continue
                    if to_date and job_date.date() > to_date.date():
                        continue
                except (ValueError, TypeError):
                    pass
            all_jobs.append(record)

    # Sort by timestamp descending (newest first)
    def _sort_key(j):
        ts = j.get("completed_at") or j.get("failed_at") or j.get("submitted_at") or ""
        return ts

    all_jobs.sort(key=_sort_key, reverse=True)
    return all_jobs


def _compute_stats(jobs: list[dict]) -> dict:
    """Compute aggregate stats from a list of jobs."""
    total = len(jobs)
    completed = sum(1 for j in jobs if j.get("status") == "saved_to_erp")
    failed = sum(1 for j in jobs if j.get("status") == "permanently_failed")
    pending_retry = sum(1 for j in jobs if j.get("status") == "pending_retry")
    active = sum(1 for j in jobs if j.get("status") in ("fetched", "submitted", "polling"))
    retried = sum(1 for j in jobs if (j.get("retry_count") or 0) > 0)

    return {
        "total": total,
        "completed": completed,
        "failed": failed,
        "pending_retry": pending_retry,
        "active": active,
        "retried": retried,
    }


# ------------------------------------------------------------------
# API Endpoints
# ------------------------------------------------------------------

@app.get("/api/stats")
def get_stats(
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
):
    """Aggregate stats, optionally filtered by date range."""
    fd = datetime.fromisoformat(from_date) if from_date else None
    td = datetime.fromisoformat(to_date) if to_date else None

    # Get all jobs (no status filter for stats)
    all_jobs = _collect_all_jobs(from_date=fd, to_date=td)
    stats = _compute_stats(all_jobs)

    # Also provide today's stats for quick reference
    today = datetime.now()
    today_jobs = _collect_all_jobs(from_date=today.replace(hour=0, minute=0, second=0), to_date=today)
    today_stats = _compute_stats(today_jobs)

    return {
        "filtered": stats,
        "today": today_stats,
    }


@app.get("/api/jobs")
def get_jobs(
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    status: str | None = Query(None),
    search: str | None = Query(None),
):
    """List jobs with optional date, status, and search filters."""
    fd = datetime.fromisoformat(from_date) if from_date else None
    td = datetime.fromisoformat(to_date) if to_date else None

    jobs = _collect_all_jobs(from_date=fd, to_date=td, status_filter=status)

    # Text search across fileName and lId
    if search:
        search_lower = search.lower()
        jobs = [
            j for j in jobs
            if search_lower in str(j.get("fileName", "")).lower()
            or search_lower in str(j.get("lId", ""))
            or search_lower in str(j.get("print_queue_job_id", "")).lower()
        ]

    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/logs")
def get_logs(
    lines: int = Query(200, ge=1, le=2000),
    search: str | None = Query(None),
    job_id: str | None = Query(None),
    file: str | None = Query(None),
):
    """Return recent log lines with optional search/filter.

    ``file`` selects a specific log file by its basename
    (e.g. ``orchestrator.log.2026-02-19``).  Defaults to the current
    ``orchestrator.log``.
    """
    if file:
        log_file = LOGS_DIR / file
    else:
        log_file = LOGS_DIR / "orchestrator.log"

    if not log_file.exists():
        return {"lines": [], "count": 0}

    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
    except Exception:
        return {"lines": [], "count": 0}

    # Take last N lines
    recent = all_lines[-lines:]

    # Filter by search term or job_id
    if search:
        recent = [l for l in recent if search.lower() in l.lower()]
    if job_id:
        recent = [l for l in recent if job_id in l]

    return {"lines": [l.rstrip() for l in recent], "count": len(recent)}


@app.get("/api/log-files")
def list_log_files():
    """Return available log files sorted newest-first."""
    files = []
    if LOGS_DIR.exists():
        for p in sorted(LOGS_DIR.iterdir(), reverse=True):
            if p.name.startswith("orchestrator.log"):
                # Derive a display label
                if p.name == "orchestrator.log":
                    label = "Today (current)"
                else:
                    # orchestrator.log.2026-02-19
                    date_part = p.name.replace("orchestrator.log.", "")
                    label = date_part
                files.append({"file": p.name, "label": label, "size": p.stat().st_size})
    return {"files": files}


# ------------------------------------------------------------------
# Timeline: parse raw logs into user-friendly milestones for a job
# ------------------------------------------------------------------

# Patterns to extract (order matters: first match wins per line)
_TIMELINE_PATTERNS = [
    # Orchestrator-level milestones
    (re.compile(r"Retrying lId=(\d+) \(attempt (\d+)/(\d+)\)"),
     lambda m: {"event": "retry", "icon": "rotate", "color": "yellow",
                "title": f"Retry attempt {m.group(2)}/{m.group(3)}",
                "detail": f"Scheduled for retry attempt {m.group(2)} of {m.group(3)}"}),

    (re.compile(r"Submitting lId=(\d+): (.+)"),
     lambda m: {"event": "submit", "icon": "paper-plane", "color": "blue",
                "title": "Submitted to Print Queue",
                "detail": m.group(2)}),

    (re.compile(r"Submitted successfully: job_id=(\S+)"),
     lambda m: {"event": "submitted_ok", "icon": "circle-check", "color": "blue",
                "title": "Print Queue accepted",
                "detail": f"Queue ID: {m.group(1)}"}),

    (re.compile(r"Polling job (\S+) for lId=(\d+)"),
     lambda m: {"event": "polling", "icon": "spinner", "color": "purple",
                "title": "Polling started",
                "detail": f"Waiting for Print Queue job {m.group(1)}"}),

    (re.compile(r"Job (\S+) completed in (\d+)s"),
     lambda m: {"event": "print_done", "icon": "print", "color": "green",
                "title": f"Printing completed ({m.group(2)}s)",
                "detail": f"Queue job {m.group(1)} finished in {m.group(2)} seconds"}),

    (re.compile(r"Poll timeout for job (\S+): (\d+)s elapsed"),
     lambda m: {"event": "timeout", "icon": "clock", "color": "red",
                "title": f"Poll timeout ({m.group(2)}s)",
                "detail": f"Queue job {m.group(1)} exceeded max wait time"}),

    (re.compile(r"Job (\S+) failed: error_type=(\w+), error=(.+)"),
     lambda m: {"event": "print_fail", "icon": "circle-xmark", "color": "red",
                "title": f"Print failed: {m.group(2)}",
                "detail": m.group(3)}),

    (re.compile(r"Job lId=(\d+) failed \[(\w+)\], scheduled for retry \((\d+)/(\d+)\): (.+)"),
     lambda m: {"event": "fail_retry", "icon": "triangle-exclamation", "color": "yellow",
                "title": f"Failed [{m.group(2)}], retry {m.group(3)}/{m.group(4)}",
                "detail": m.group(5)}),

    (re.compile(r"Job lId=(\d+) permanently failed"),
     lambda m: {"event": "perm_fail", "icon": "circle-xmark", "color": "red",
                "title": "Permanently failed",
                "detail": "Max retries exhausted"}),

    (re.compile(r"Saving flattened doc payload:.*'lId': (\d+)"),
     lambda m: {"event": "erp_save", "icon": "cloud-arrow-up", "color": "teal",
                "title": "Uploading to ERP",
                "detail": "Flattened document being saved to ERP"}),

    (re.compile(r"ERP save successful for lId=(\d+)"),
     lambda m: {"event": "erp_ok", "icon": "circle-check", "color": "green",
                "title": "Saved to ERP",
                "detail": "Flattened document stored in ERP successfully"}),

    (re.compile(r"Moved doc_(\d+) to completed"),
     lambda m: {"event": "completed", "icon": "flag-checkered", "color": "green",
                "title": "Completed",
                "detail": "Job moved to completed state"}),

    (re.compile(r"Successfully saved flattened doc for lId=(\d+)"),
     lambda m: {"event": "success", "icon": "star", "color": "green",
                "title": "Pipeline finished",
                "detail": "Entire flatten pipeline successful"}),
]

# Log line timestamp regex
_LOG_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


@app.get("/api/timeline")
def get_timeline(lid: str = Query(...)):
    """Parse logs for a given lId and return user-friendly milestones."""
    milestones = []

    # Search all log files for this lId
    log_files = []
    if LOGS_DIR.exists():
        for p in sorted(LOGS_DIR.iterdir()):
            if p.name.startswith("orchestrator.log"):
                log_files.append(p)

    for log_file in log_files:
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if lid not in line:
                        continue
                    # Skip DEBUG and non-relevant loggers
                    if "| DEBUG" in line:
                        continue

                    # Extract timestamp
                    ts_match = _LOG_TS.match(line)
                    ts = ts_match.group(1) if ts_match else ""

                    # Try each pattern
                    for pattern, builder in _TIMELINE_PATTERNS:
                        match = pattern.search(line)
                        if match:
                            milestone = builder(match)
                            milestone["timestamp"] = ts
                            milestones.append(milestone)
                            break
        except Exception:
            continue

    return {"milestones": milestones, "count": len(milestones)}


@app.get("/api/chart")
def get_chart_data(
    frm: str | None = Query(None, alias="from"),
    to: str | None = Query(None),
):
    """Return daily completed/failed counts for charting, driven by main date filter."""
    today = datetime.now().date()

    # Determine date range from params (default: last 14 days)
    try:
        start_date = datetime.strptime(frm, "%Y-%m-%d").date() if frm else today - timedelta(days=13)
    except ValueError:
        start_date = today - timedelta(days=13)
    try:
        end_date = datetime.strptime(to, "%Y-%m-%d").date() if to else today
    except ValueError:
        end_date = today

    # Build daily buckets
    daily = {}
    d = start_date
    while d <= end_date:
        daily[d.isoformat()] = {"completed": 0, "failed": 0}
        d += timedelta(days=1)

    # Walk state folders
    if STATES_DIR.exists():
        for month_dir in sorted(STATES_DIR.iterdir()):
            if not month_dir.is_dir() or "_" not in month_dir.name:
                continue
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir():
                    continue
                folder_date = _parse_date_folder(month_dir.name, day_dir.name)
                if not folder_date:
                    continue
                key = folder_date.date().isoformat()
                if key not in daily:
                    continue

                completed = _read_json(str(day_dir / "completed.json")) or {}
                failed = _read_json(str(day_dir / "failed.json")) or {}
                daily[key]["completed"] = len(completed)
                daily[key]["failed"] = len(failed)

    labels = list(daily.keys())
    completed = [daily[d]["completed"] for d in labels]
    failed = [daily[d]["failed"] for d in labels]

    return {"labels": labels, "completed": completed, "failed": failed}


# ------------------------------------------------------------------
# Serve Frontend
# ------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    index_path = UI_DIR / "index.html"
    return FileResponse(str(index_path))


# ------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=" * 50)
    print("Flatten Orchestrator Dashboard")
    print(f"States dir: {STATES_DIR}")
    print(f"Logs dir:   {LOGS_DIR}")
    print(f"UI dir:     {UI_DIR}")
    print("=" * 50)
    uvicorn.run(app, host="127.0.0.1", port=8050, log_level="info")
