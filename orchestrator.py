"""
Core pipeline logic for the Flatten Orchestrator.
Implements: retry phase -> fetch phase -> process phase (batched)

File lifecycle:
  - Input PDFs:     cache/input_{lId}_{filename}     — deleted after successful ERP save
  - Flattened PDFs: cache/flatten_{lId}_{filename}    — deleted after successful ERP save
  - On permanent failure: files are KEPT for manual inspection

Batch processing:
  - Documents are processed in batches of BATCH_SIZE (default 10)
  - Each batch uses a sliding window (depth 2):
    - 2 jobs are submitted upfront to keep the Print API worker saturated
    - Polling occurs sequentially: poll timeout starts only after the prior job resolves
    - On each resolution, the next job is submitted to maintain queue depth
  - Eliminates false timeouts caused by queue wait time
"""
import os
import base64
import logging
from datetime import datetime

from config import config
from models import TrackedJob, SaveFlattenDocRequest
from state_manager import StateManager
from erp_client import ERPClient
from print_queue_client import PrintQueueClient

logger = logging.getLogger(__name__)


class Orchestrator:
    """Core pipeline: fetch documents, submit for flattening, save results"""

    def __init__(self):
        self.state = StateManager()
        self.erp = ERPClient()
        self.print_queue = PrintQueueClient()
        self.cache_dir = config.CACHE_DIR
        self.batch_size = config.BATCH_SIZE

    def run_cycle(self) -> dict:
        """Execute one full cycle: retry -> fetch -> process (batched).

        Returns:
            Summary dict with counts of each action taken
        """
        summary = {
            "retried": 0,
            "fetched": 0,
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "saved_to_erp": 0,
        }

        logger.info("=" * 60)
        logger.info("CYCLE START")
        logger.info("=" * 60)

        # --- Phase 1: Retry pending jobs ---
        retry_results = self._retry_phase()
        summary["retried"] = retry_results["retried"]
        summary["completed"] += retry_results["completed"]
        summary["failed"] += retry_results["failed"]
        summary["saved_to_erp"] += retry_results["saved_to_erp"]

        # --- Phase 2: Fetch new documents ---
        new_docs = self._fetch_phase()
        summary["fetched"] = len(new_docs)

        # --- Phase 3: Process in batches ---
        for batch_start in range(0, len(new_docs), self.batch_size):
            batch = new_docs[batch_start : batch_start + self.batch_size]
            batch_num = (batch_start // self.batch_size) + 1
            total_batches = (len(new_docs) + self.batch_size - 1) // self.batch_size

            logger.info(
                f"PROCESS PHASE: Batch {batch_num}/{total_batches} "
                f"({len(batch)} jobs) — Sliding window (depth 2)"
            )

            batch_results = self._process_jobs_sliding_window(batch)
            summary["submitted"] += batch_results["submitted"]
            summary["completed"] += batch_results["completed"]
            summary["failed"] += batch_results["failed"]
            summary["saved_to_erp"] += batch_results["saved_to_erp"]

        # --- Update timestamp ---
        self.state.set_last_fetch_timestamp(datetime.now().isoformat())

        logger.info("=" * 60)
        logger.info(f"CYCLE COMPLETE: {summary}")
        logger.info("=" * 60)

        return summary


    # ------------------------------------------------------------------
    # Phase 1: Retry
    # ------------------------------------------------------------------

    def _retry_phase(self) -> dict:
        """Re-process jobs that are pending_retry"""
        results = {"retried": 0, "completed": 0, "failed": 0, "saved_to_erp": 0}

        pending_jobs = self.state.get_jobs_by_status("pending_retry")
        if not pending_jobs:
            logger.info("RETRY PHASE: No pending retries")
            return results

        logger.info(f"RETRY PHASE: {len(pending_jobs)} jobs to retry")

        return self._process_jobs_sliding_window(pending_jobs)

    def _process_jobs_sliding_window(self, jobs: list[TrackedJob]) -> dict:
        """Process jobs using a sliding window of depth 2.

        The Print API has a single worker. This method keeps 2 jobs queued
        so the worker never idles between jobs, while polling sequentially
        to ensure the timeout clock starts only after the prior job resolves.

        Flow:
          1. Submit up to 2 jobs immediately.
          2. Poll the oldest submitted job until it resolves.
          3. Handle the result (ERP save / retry / fail).
          4. Submit the next pending job (maintaining queue depth of 2).
          5. Repeat from step 2 until all jobs are resolved.
        """
        results = {"retried": 0, "submitted": 0, "completed": 0, "failed": 0, "saved_to_erp": 0}

        if not jobs:
            return results

        pending = list(jobs)       # Jobs waiting to be submitted
        submitted = []             # (job, job_id) pairs submitted but not yet polled

        # --- Helper: submit one job from pending, append to submitted ---
        def _submit_next():
            if not pending:
                return
            job = pending.pop(0)

            if job.status == "pending_retry":
                logger.info(
                    f"Retrying lId={job.lId} (attempt {job.retry_count + 1}/{config.MAX_RETRIES})"
                )
                results["retried"] += 1

            logger.info(f"Submitting lId={job.lId}: {job.fileName}")
            submit_result, submit_error_type = self.print_queue.submit_job(
                job.fileName, job.input_file_path
            )

            if not submit_result:
                logger.error(f"Submit failed for lId={job.lId}")
                self._handle_failure(job, "Failed to submit to print queue", submit_error_type or "unknown")
                results["submitted"] += 1
                results["failed"] += 1
                # Try to submit the next job in its place
                _submit_next()
                return

            # Update state with job ID
            self.state.update_active_job(
                job.lId,
                print_queue_job_id=submit_result.job_id,
                status="submitted",
                submitted_at=datetime.now().isoformat(),
            )
            submitted.append((job, submit_result.job_id))
            results["submitted"] += 1

        # --- Seed the window: submit first 2 jobs ---
        _submit_next()
        _submit_next()

        # --- Process until all submitted jobs are resolved ---
        while submitted:
            job, job_id = submitted.pop(0)

            # Poll this job (timeout clock starts NOW)
            logger.info(f"Polling job {job_id} for lId={job.lId}")
            self.state.update_active_job(job.lId, status="polling")
            poll_result = self.print_queue.poll_until_complete(job_id)

            # Handle result
            if poll_result.status == "completed":
                outcome = self._save_to_erp(job, poll_result)
                if outcome == "saved_to_erp":
                    results["completed"] += 1
                    results["saved_to_erp"] += 1
                else:
                    results["failed"] += 1
            else:
                self._handle_failure(job, poll_result.error, poll_result.error_type)
                results["failed"] += 1

            # Refill window: submit next pending job
            _submit_next()

        return results

    # ------------------------------------------------------------------
    # Phase 2: Fetch
    # ------------------------------------------------------------------

    def _fetch_phase(self) -> list[TrackedJob]:
        """Fetch new documents from ERP and save to cache"""
        from_date = self.state.get_last_fetch_timestamp()

        # Default to configured initial timestamp if no previous timestamp
        if not from_date:
            from_date = "2026-02-16T13:09:01.300"
            logger.info(f"No previous timestamp found, using initial: {from_date}")

        try:
            documents = self.erp.fetch_documents(from_date)
        except Exception as e:
            logger.error(f"FETCH PHASE: Failed to fetch documents: {e}")
            return []

        new_jobs = []
        for doc in documents:
            # Deduplication
            if self.state.is_document_tracked(doc.lId):
                logger.debug(f"Skipping already tracked document lId={doc.lId}")
                continue

            # Resolve flatten doc type
            try:
                flatten_doc_type = config.get_flatten_doc_type(doc.docType)
            except KeyError:
                logger.warning(
                    f"No flatten mapping for docType={doc.docType}, skipping lId={doc.lId}"
                )
                continue

            # --- Validate base64 content ---
            if not doc.fileBase64 or not doc.fileBase64.strip():
                logger.warning(
                    f"Empty or blank fileBase64 for lId={doc.lId} "
                    f"(docType={doc.docType}, fileName={doc.fileName}), skipping"
                )
                continue

            # --- Save input PDF to cache ---
            input_path = self._save_to_cache(doc.lId, doc.fileName, doc.fileBase64, prefix="input")
            if not input_path:
                logger.error(f"Failed to cache input file for lId={doc.lId}, skipping")
                continue

            job = TrackedJob(
                lId=doc.lId,
                jobId=doc.jobId,
                docPath=doc.docPath,
                fileName=doc.fileName,
                original_docType=doc.docType,
                flatten_docType=flatten_doc_type,
                input_file_path=input_path,
                status="fetched",
            )

            self.state.add_active_job(job)
            new_jobs.append(job)

        logger.info(f"FETCH PHASE: {len(new_jobs)} new documents to process")
        return new_jobs

    # ------------------------------------------------------------------
    # Phase 3: Process
    # ------------------------------------------------------------------

    # _process_single_job and _submit_and_poll removed:
    # Submit and poll logic is now inlined in _process_jobs_sliding_window
    # to enable the sliding window pattern (deferred polling).

    # ------------------------------------------------------------------
    # ERP Save + Cleanup
    # ------------------------------------------------------------------

    def _save_to_erp(self, job: TrackedJob, poll_result) -> str:
        """Save the flattened PDF to cache, send to ERP, cleanup on success."""
        if not poll_result.result:
            logger.error(f"No base64 result in completed job for lId={job.lId}")
            self._handle_failure(job, "Completed but no base64 in response", "unknown")
            return "failed"

        # --- Save flattened PDF to cache ---
        base_name = job.fileName.rsplit(".", 1)[0] if "." in job.fileName else job.fileName
        flatten_filename = f"{base_name}_flatten.pdf"

        output_path = self._save_to_cache(
            job.lId, flatten_filename, poll_result.result, prefix="flatten"
        )
        if not output_path:
            logger.error(f"Failed to cache flattened output for lId={job.lId}")
            self._handle_failure(job, "Failed to write flattened file to cache", "unknown")
            return "failed"

        self.state.update_active_job(job.lId, output_file_path=output_path)

        # --- Send to ERP ---
        request = SaveFlattenDocRequest(
            lId=job.lId,
            jobId=job.jobId,
            docPath=job.docPath,
            fileName=flatten_filename,
            docType=job.flatten_docType,
            flattenQueueId=job.print_queue_job_id or "",
            flattenStatusId=2,
            fileBase64=poll_result.result,
        )

        success = self.erp.save_flatten_doc(request)

        if success:
            # --- Cleanup cached files ONLY after confirmed ERP success ---
            self._cleanup_cached_files(job.lId)

            self.state.update_active_job(
                job.lId,
                status="saved_to_erp",
                completed_at=datetime.now().isoformat(),
            )
            self.state.move_to_completed(job.lId)
            logger.info(f"Successfully saved flattened doc for lId={job.lId}")
            return "saved_to_erp"
        else:
            self._handle_failure(job, "Failed to save flattened doc to ERP", "unknown")
            return "failed"

    # ------------------------------------------------------------------
    # File Cache Helpers
    # ------------------------------------------------------------------

    def _save_to_cache(
        self, lId: int, filename: str, file_base64: str, prefix: str
    ) -> str | None:
        """Decode base64 and save PDF to cache directory."""
        cache_filename = f"{prefix}_{lId}_{filename}"
        cache_path = os.path.join(self.cache_dir, cache_filename)

        try:
            file_bytes = base64.b64decode(file_base64)
            with open(cache_path, "wb") as f:
                f.write(file_bytes)
            logger.debug(f"Cached {prefix} file: {cache_path} ({len(file_bytes)} bytes)")
            return cache_path
        except Exception as e:
            logger.error(f"Failed to cache {prefix} file for lId={lId}: {e}")
            return None

    def _cleanup_cached_files(self, lId: int) -> None:
        """Delete cached input and output files after confirmed ERP save success."""
        job = self.state.get_active_job(lId)
        if not job:
            return

        for path_attr in ("input_file_path", "output_file_path"):
            file_path = getattr(job, path_attr, None)
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.debug(f"Cleaned up cached file: {file_path}")
                except OSError as e:
                    logger.warning(f"Could not delete cached file {file_path}: {e}")

    # ------------------------------------------------------------------
    # Error Handling
    # ------------------------------------------------------------------

    def _handle_failure(self, job: TrackedJob, error: str, error_type: str | None) -> None:
        """Handle a failed job: retry or permanently fail.
        Cached files are KEPT on failure for debugging/manual retry.
        """
        error_type = error_type or "unknown"

        # Refresh job state (retry_count may have been updated)
        current_job = self.state.get_active_job(job.lId)
        retry_count = current_job.retry_count if current_job else job.retry_count

        new_retry_count = retry_count + 1

        if error_type in config.RETRYABLE_ERRORS and new_retry_count < config.MAX_RETRIES:
            # Schedule for retry — files stay on disk
            self.state.update_active_job(
                job.lId,
                status="pending_retry",
                error=error,
                error_type=error_type,
                retry_count=new_retry_count,
            )
            logger.warning(
                f"Job lId={job.lId} failed [{error_type}], scheduled for retry "
                f"({new_retry_count}/{config.MAX_RETRIES}): {error}"
            )
        else:
            # Permanent failure — files KEPT for manual inspection
            self.state.update_active_job(
                job.lId,
                status="permanently_failed",
                error=error,
                error_type=error_type,
                retry_count=new_retry_count,
                completed_at=datetime.now().isoformat(),
            )
            self.state.move_to_failed(job.lId)

            reason = "non-retryable" if error_type not in config.RETRYABLE_ERRORS else "max retries exceeded"
            logger.error(
                f"Job lId={job.lId} permanently failed [{error_type}] ({reason}): {error}"
            )
