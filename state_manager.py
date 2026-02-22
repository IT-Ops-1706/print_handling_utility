"""
JSON-based state persistence for the Flatten Orchestrator.

File layout:
  States/
    metadata.json              — last_fetch_timestamp
    active_jobs.json           — in-progress and pending_retry jobs (operational)
    processed_lids.json        — append-only set of all processed lId values (dedup)
    {Mon_YYYY}/
      {DD_MM}/
        completed.json         — completed jobs for that date
        failed.json            — permanently failed jobs for that date

Writes are atomic: temp file + os.replace() to prevent corruption.
"""
import os
import json
import logging
import tempfile
import threading
from datetime import datetime

from config import config
from models import OrchestratorState, TrackedJob

logger = logging.getLogger(__name__)


class StateManager:
    """Manages persistent state across split date-wise JSON files"""

    def __init__(self, states_dir: str = None):
        self.states_dir = states_dir or config.STATES_DIR
        os.makedirs(self.states_dir, exist_ok=True)

        self._metadata_file = os.path.join(self.states_dir, "metadata.json")
        self._active_file = os.path.join(self.states_dir, "active_jobs.json")
        self._lids_file = os.path.join(self.states_dir, "processed_lids.json")

        self.state = self._load()
        self._processed_lids: set[int] = self._load_processed_lids()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def _load(self) -> OrchestratorState:
        """Load metadata + active jobs. Returns fresh state if missing/corrupt."""
        metadata = self._read_json(self._metadata_file) or {}
        active_raw = self._read_json(self._active_file) or {}

        active_jobs = {}
        recovered = 0
        for key, raw in active_raw.items():
            try:
                job = TrackedJob(**raw)
                # Crash recovery: polling jobs were interrupted
                if job.status == "polling":
                    job.status = "pending_retry"
                    job.error = "Process interrupted during polling"
                    job.error_type = "process_crash"
                    recovered += 1
                active_jobs[key] = job
            except Exception as e:
                logger.warning(f"Skipping corrupt active job {key}: {e}")

        if recovered:
            logger.warning(f"Recovered {recovered} interrupted jobs to pending_retry")

        logger.info(f"Loaded state: {len(active_jobs)} active jobs")
        return OrchestratorState(
            last_fetch_timestamp=metadata.get("last_fetch_timestamp"),
            active_jobs=active_jobs,
        )

    def _load_processed_lids(self) -> set[int]:
        """Load the set of all processed lId values for deduplication."""
        data = self._read_json(self._lids_file)
        if data and isinstance(data, list):
            return set(data)
        return set()

    # ------------------------------------------------------------------
    # Save helpers
    # ------------------------------------------------------------------

    def _read_json(self, path: str) -> dict | list | None:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read {path}: {e}")
            return None

    def _write_json_atomic(self, path: str, data: dict | list) -> None:
        """Write JSON atomically: temp file + os.replace()."""
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _save_active(self) -> None:
        """Persist active_jobs.json atomically."""
        data = {k: v.model_dump() for k, v in self.state.active_jobs.items()}
        self._write_json_atomic(self._active_file, data)

    def _save_metadata(self) -> None:
        """Persist metadata.json atomically."""
        self._write_json_atomic(self._metadata_file, {
            "last_fetch_timestamp": self.state.last_fetch_timestamp,
        })

    def _save_processed_lids(self) -> None:
        """Persist processed_lids.json atomically."""
        self._write_json_atomic(self._lids_file, sorted(self._processed_lids))

    # ------------------------------------------------------------------
    # Date-wise folder helpers
    # ------------------------------------------------------------------

    def _date_folder(self, dt: datetime = None) -> str:
        """Return path to States/Mon_YYYY/DD_MM/ for the given datetime."""
        dt = dt or datetime.now()
        month_folder = dt.strftime("%b_%Y")   # e.g. Feb_2026
        day_folder = dt.strftime("%d_%m")     # e.g. 19_02
        return os.path.join(self.states_dir, month_folder, day_folder)

    def _append_to_date_file(self, filename: str, key: str, record: dict) -> None:
        """Append a single record to a date-wise JSON file.

        The file stores a dict keyed by doc key (e.g. 'doc_2391549').
        """
        folder = self._date_folder()
        path = os.path.join(folder, filename)

        existing = self._read_json(path) or {}
        existing[key] = record
        self._write_json_atomic(path, existing)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_last_fetch_timestamp(self) -> str | None:
        return self.state.last_fetch_timestamp

    def set_last_fetch_timestamp(self, timestamp: str) -> None:
        self.state.last_fetch_timestamp = timestamp
        self._save_metadata()

    def is_document_tracked(self, lId: int) -> bool:
        """Check if a document is already tracked (active or previously processed)."""
        key = f"doc_{lId}"
        return key in self.state.active_jobs or lId in self._processed_lids

    def add_active_job(self, job: TrackedJob) -> None:
        with self._lock:
            key = f"doc_{job.lId}"
            self.state.active_jobs[key] = job
            self._save_active()
            logger.debug(f"Added active job: {key} status={job.status}")

    def update_active_job(self, lId: int, **kwargs) -> None:
        """Update fields on an active job."""
        with self._lock:
            key = f"doc_{lId}"
            job = self.state.active_jobs.get(key)
            if not job:
                logger.warning(f"Cannot update: job {key} not found in active_jobs")
                return
            for field, value in kwargs.items():
                setattr(job, field, value)
            self._save_active()

    def get_active_job(self, lId: int) -> TrackedJob | None:
        with self._lock:
            return self.state.active_jobs.get(f"doc_{lId}")

    def get_jobs_by_status(self, status: str) -> list[TrackedJob]:
        """Return all active jobs with the given status."""
        with self._lock:
            return [j for j in self.state.active_jobs.values() if j.status == status]

    def move_to_completed(self, lId: int) -> None:
        with self._lock:
            key = f"doc_{lId}"
            job = self.state.active_jobs.pop(key, None)
            if not job:
                return

            record = {
                "lId": job.lId,
                "jobId": job.jobId,
                "fileName": job.fileName,
                "print_queue_job_id": job.print_queue_job_id,
                "flatten_docType": job.flatten_docType,
                "status": "saved_to_erp",
                "completed_at": datetime.now().isoformat(),
            }
            self._append_to_date_file("completed.json", key, record)
            self._register_processed(lId)
            self._save_active()
            logger.info(f"Moved {key} to completed ({self._date_folder_label()})")

    def move_to_failed(self, lId: int) -> None:
        with self._lock:
            key = f"doc_{lId}"
            job = self.state.active_jobs.pop(key, None)
            if not job:
                return

            record = {
                "lId": job.lId,
                "jobId": job.jobId,
                "fileName": job.fileName,
                "error": job.error,
                "error_type": job.error_type,
                "retry_count": job.retry_count,
                "status": "permanently_failed",
                "failed_at": datetime.now().isoformat(),
            }
            self._append_to_date_file("failed.json", key, record)
            self._register_processed(lId)
            self._save_active()
            logger.warning(f"Moved {key} to permanently_failed ({self._date_folder_label()})")

    def get_today_stats(self) -> dict:
        """Read today's completed and failed files for the daily report."""
        folder = self._date_folder()
        completed = self._read_json(os.path.join(folder, "completed.json")) or {}
        failed = self._read_json(os.path.join(folder, "failed.json")) or {}
        active = self.state.active_jobs

        failed_details = [
            {
                "lId": v.get("lId", ""),
                "fileName": v.get("fileName", k),
                "error_type": v.get("error_type", ""),
                "error": v.get("error", ""),
            }
            for k, v in failed.items()
        ]

        return {
            "total_processed": len(completed) + len(failed),
            "completed": len(completed),
            "failed": len(failed),
            "in_queue": len([j for j in active.values() if j.status in ("fetched", "submitted", "polling")]),
            "pending_retry": len([j for j in active.values() if j.status == "pending_retry"]),
            "permanently_failed": len(failed),
            "failed_details": failed_details,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _register_processed(self, lId: int) -> None:
        """Add lId to the processed set and persist."""
        self._processed_lids.add(lId)
        self._save_processed_lids()

    def _date_folder_label(self) -> str:
        dt = datetime.now()
        return f"{dt.strftime('%b_%Y')}/{dt.strftime('%d_%m')}"
