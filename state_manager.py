"""
JSON-based state persistence for the Flatten Orchestrator.
Atomic writes via temp file + os.replace() to prevent corruption.
"""
import os
import json
import logging
import tempfile
from datetime import datetime

from config import config
from models import OrchestratorState, TrackedJob

logger = logging.getLogger(__name__)


class StateManager:
    """Manages persistent state in state.json with atomic writes"""

    def __init__(self, state_file: str = None):
        self.state_file = state_file or config.STATE_FILE
        self.state = self._load()

    def _load(self) -> OrchestratorState:
        """Load state from disk. Returns empty state if file missing or corrupt."""
        if not os.path.exists(self.state_file):
            logger.info("No existing state file found. Starting fresh.")
            return OrchestratorState()

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            state = OrchestratorState(**data)
            logger.info(
                f"Loaded state: {len(state.active_jobs)} active, "
                f"{len(state.completed_jobs)} completed, "
                f"{len(state.failed_jobs)} failed"
            )

            # Crash recovery: mark any POLLING jobs as pending_retry
            recovered = 0
            for key, job in state.active_jobs.items():
                if job.status == "polling":
                    job.status = "pending_retry"
                    job.error = "Process interrupted during polling"
                    job.error_type = "process_crash"
                    recovered += 1
            if recovered > 0:
                logger.warning(f"Recovered {recovered} interrupted jobs to pending_retry")

            return state

        except Exception as e:
            logger.error(f"Failed to load state file: {e}. Starting fresh.")
            return OrchestratorState()

    def save(self) -> None:
        """Persist state to disk atomically"""
        try:
            state_dir = os.path.dirname(self.state_file)
            fd, tmp_path = tempfile.mkstemp(dir=state_dir, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self.state.model_dump(), f, indent=2, default=str)
                os.replace(tmp_path, self.state_file)
            except Exception:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            raise

    # --- Convenience methods ---

    def get_last_fetch_timestamp(self) -> str | None:
        return self.state.last_fetch_timestamp

    def set_last_fetch_timestamp(self, timestamp: str) -> None:
        self.state.last_fetch_timestamp = timestamp
        self.save()

    def is_document_tracked(self, lId: int) -> bool:
        """Check if a document is already tracked (active, completed, or failed)"""
        key = f"doc_{lId}"
        return (
            key in self.state.active_jobs
            or key in self.state.completed_jobs
            or key in self.state.failed_jobs
        )

    def add_active_job(self, job: TrackedJob) -> None:
        key = f"doc_{job.lId}"
        self.state.active_jobs[key] = job
        self.save()
        logger.debug(f"Added active job: {key} status={job.status}")

    def update_active_job(self, lId: int, **kwargs) -> None:
        """Update fields on an active job"""
        key = f"doc_{lId}"
        job = self.state.active_jobs.get(key)
        if not job:
            logger.warning(f"Cannot update: job {key} not found in active_jobs")
            return
        for field, value in kwargs.items():
            setattr(job, field, value)
        self.save()

    def get_active_job(self, lId: int) -> TrackedJob | None:
        return self.state.active_jobs.get(f"doc_{lId}")

    def get_jobs_by_status(self, status: str) -> list[TrackedJob]:
        """Return all active jobs with the given status"""
        return [
            job for job in self.state.active_jobs.values()
            if job.status == status
        ]

    def move_to_completed(self, lId: int) -> None:
        key = f"doc_{lId}"
        job = self.state.active_jobs.pop(key, None)
        if job:
            self.state.completed_jobs[key] = {
                "lId": job.lId,
                "jobId": job.jobId,
                "print_queue_job_id": job.print_queue_job_id,
                "status": "saved_to_erp",
                "completed_at": datetime.now().isoformat(),
            }
            self.save()
            logger.info(f"Moved {key} to completed")

    def move_to_failed(self, lId: int) -> None:
        key = f"doc_{lId}"
        job = self.state.active_jobs.pop(key, None)
        if job:
            self.state.failed_jobs[key] = {
                "lId": job.lId,
                "jobId": job.jobId,
                "error": job.error,
                "error_type": job.error_type,
                "retry_count": job.retry_count,
                "status": "permanently_failed",
                "failed_at": datetime.now().isoformat(),
            }
            self.save()
            logger.warning(f"Moved {key} to permanently_failed")
