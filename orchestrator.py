"""
Core pipeline logic for the Flatten Orchestrator.
Implements: retry phase -> fetch phase -> process phase (batched)

File lifecycle:
  - Input PDFs:     cache/input_{lId}_{filename}     — deleted after successful ERP save
  - Flattened PDFs: cache/flatten_{lId}_{filename}    — deleted after successful ERP save
  - On permanent failure: files are KEPT for manual inspection

Batch processing:
  - Documents are processed in batches of BATCH_SIZE (default 10)
  - Each batch is submitted sequentially (one at a time through Print Queue)
  - Next batch starts after current batch completes
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
from email_notifier import EmailNotifier

logger = logging.getLogger(__name__)


class Orchestrator:
    """Core pipeline: fetch documents, submit for flattening, save results"""

    def __init__(self):
        self.state = StateManager()
        self.erp = ERPClient()
        self.print_queue = PrintQueueClient()
        self.email = EmailNotifier()
        self.cache_dir = config.CACHE_DIR
        self.batch_size = config.BATCH_SIZE

        if self.email.is_configured():
            logger.info("Email notifications enabled")
        else:
            logger.warning("Email notifications NOT configured (missing env vars)")

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
                f"({len(batch)} jobs)"
            )

            for doc_job in batch:
                result = self._process_single_job(doc_job)
                summary["submitted"] += 1
                if result == "saved_to_erp":
                    summary["completed"] += 1
                    summary["saved_to_erp"] += 1
                elif result == "failed":
                    summary["failed"] += 1

        # --- Update timestamp ---
        self.state.set_last_fetch_timestamp(datetime.now().isoformat())

        logger.info("=" * 60)
        logger.info(f"CYCLE COMPLETE: {summary}")
        logger.info("=" * 60)

        return summary

    def get_daily_report_stats(self) -> dict:
        """Build stats for the daily email report from current state"""
        state = self.state.state

        # Collect failed job details
        failed_details = []
        for key, job_data in state.failed_jobs.items():
            failed_details.append({
                "lId": job_data.get("lId", ""),
                "fileName": job_data.get("fileName", key),
                "error_type": job_data.get("error_type", ""),
                "error": job_data.get("error", ""),
            })

        return {
            "total_processed": len(state.completed_jobs) + len(state.failed_jobs),
            "completed": len(state.completed_jobs),
            "failed": len(state.failed_jobs),
            "in_queue": len([
                j for j in state.active_jobs.values()
                if j.status in ("fetched", "submitted", "polling")
            ]),
            "pending_retry": len([
                j for j in state.active_jobs.values()
                if j.status == "pending_retry"
            ]),
            "permanently_failed": len(state.failed_jobs),
            "failed_details": failed_details,
        }

    def send_daily_report(self) -> bool:
        """Trigger the daily email report"""
        if not self.email.is_configured():
            logger.debug("Email not configured, skipping daily report")
            return False

        stats = self.get_daily_report_stats()
        return self.email.send_daily_report(stats)

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

        for job in pending_jobs:
            results["retried"] += 1
            logger.info(
                f"Retrying lId={job.lId} (attempt {job.retry_count + 1}/{config.MAX_RETRIES})"
            )

            # Verify input file still exists on disk
            if not job.input_file_path or not os.path.exists(job.input_file_path):
                logger.error(f"Input file missing for retry lId={job.lId}: {job.input_file_path}")
                self._handle_failure(job, "Input file missing on disk", "file_validation_failed")
                results["failed"] += 1
                continue

            result = self._submit_and_poll(job)
            if result == "saved_to_erp":
                results["completed"] += 1
                results["saved_to_erp"] += 1
            elif result == "failed":
                results["failed"] += 1

        return results

    # ------------------------------------------------------------------
    # Phase 2: Fetch
    # ------------------------------------------------------------------

    def _fetch_phase(self) -> list[TrackedJob]:
        """Fetch new documents from ERP and save to cache"""
        last_ts = self.state.get_last_fetch_timestamp()

        # Default to today if no previous timestamp
        from_date = last_ts if last_ts else datetime.now().strftime("%Y-%m-%d")

        # Use only the date portion for the API call
        if "T" in from_date:
            from_date = from_date.split("T")[0]

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

    def _process_single_job(self, job: TrackedJob) -> str:
        """Process a single document through submit -> poll -> save."""
        return self._submit_and_poll(job)

    def _submit_and_poll(self, job: TrackedJob) -> str:
        """Submit a job to the Print Queue and poll for completion.

        Returns:
            "saved_to_erp" on success, "failed" on failure
        """
        # --- Submit (read from disk) ---
        logger.info(f"Submitting lId={job.lId}: {job.fileName}")
        submit_result = self.print_queue.submit_job(job.fileName, job.input_file_path)

        if not submit_result:
            logger.error(f"Submit failed for lId={job.lId}")
            self._handle_failure(job, "Failed to submit to print queue", "unknown")
            return "failed"

        # Update state with job ID
        self.state.update_active_job(
            job.lId,
            print_queue_job_id=submit_result.job_id,
            status="polling",
            submitted_at=datetime.now().isoformat(),
        )

        # --- Poll (with timeout) ---
        logger.info(f"Polling job {submit_result.job_id} for lId={job.lId}")
        poll_result = self.print_queue.poll_until_complete(submit_result.job_id)

        if poll_result.status == "completed":
            return self._save_to_erp(job, poll_result)
        else:
            self._handle_failure(job, poll_result.error, poll_result.error_type)
            return "failed"

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
            flattenStatusId=1,
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
        Sends email alert on permanent failure.
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
            # Permanent failure — files KEPT, send email alert
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

            # --- Send failure alert email ---
            if self.email.is_configured():
                try:
                    self.email.send_failure_alert(
                        lId=job.lId,
                        fileName=job.fileName,
                        error=error or "",
                        error_type=error_type,
                        retry_count=new_retry_count,
                    )
                except Exception as e:
                    logger.error(f"Failed to send failure alert email for lId={job.lId}: {e}")
