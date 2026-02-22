"""
HTTP client for the Print Queue API.
Handles job submission and status polling.
"""
import logging
import time
import httpx

from config import config
from models import PrintQueueResponse, PrintJobStatus

logger = logging.getLogger(__name__)


class PrintQueueClient:
    """HTTP client for the Print Queue API"""

    def __init__(self):
        self.base_url = config.PRINT_QUEUE_URL
        self.headers = {
            "Authorization": f"Bearer {config.PRINT_API_KEY}",
        }
        self.timeout = config.HTTP_TIMEOUT_SECONDS
        self.poll_interval = config.POLL_INTERVAL_SECONDS

    def submit_job(self, filename: str, file_path: str) -> tuple[PrintQueueResponse | None, str | None]:
        """Submit a PDF to the Print Queue for flattening.

        Args:
            filename: Original filename of the PDF
            file_path: Path to the cached PDF file on disk

        Returns:
            (PrintQueueResponse, None) on success
            (None, error_type) on failure — error_type indicates if retryable
        """
        url = f"{self.base_url}/print-queue"

        logger.info(f"Submitting to Print Queue: {filename} (from {file_path})")

        try:
            with open(file_path, "rb") as f:
                response = httpx.post(
                    url,
                    headers=self.headers,
                    files={"file": (filename, f, "application/pdf")},
                    timeout=self.timeout,
                )
            response.raise_for_status()
            result = PrintQueueResponse(**response.json())
            logger.info(f"Submitted successfully: job_id={result.job_id}")
            return result, None

        except FileNotFoundError:
            logger.error(f"Cached input file not found: {file_path}")
            return None, "file_not_found"
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Print Queue submit HTTP error for {filename}: "
                f"{e.response.status_code} - {e.response.text}"
            )
            return None, "unknown"
        except httpx.ConnectError as e:
            logger.error(f"Print Queue connection error for {filename}: {e}")
            return None, "print_queue_unavailable"
        except httpx.RequestError as e:
            logger.error(f"Print Queue request error for {filename}: {e}")
            return None, "print_queue_unavailable"
        except Exception as e:
            logger.error(f"Unexpected error submitting {filename}: {e}")
            return None, "unknown"

    def poll_until_complete(self, job_id: str) -> PrintJobStatus:
        """Poll the Print Queue until the job completes, fails, or times out.

        Timeout: MAX_POLL_TIMEOUT_SECONDS (default 90s).

        Args:
            job_id: The job ID returned from submit_job

        Returns:
            PrintJobStatus with final status (completed, failed, or timeout)
        """
        url = f"{self.base_url}/job-status/{job_id}"
        max_timeout = config.MAX_POLL_TIMEOUT_SECONDS
        start_time = time.monotonic()

        logger.info(f"Polling job {job_id} every {self.poll_interval}s (timeout: {max_timeout}s)...")

        while True:
            elapsed = time.monotonic() - start_time

            # --- Timeout guard ---
            if elapsed >= max_timeout:
                logger.error(
                    f"Poll timeout for job {job_id}: {elapsed:.0f}s elapsed "
                    f"(max {max_timeout}s)"
                )
                return PrintJobStatus(
                    id=job_id,
                    filename="",
                    status="failed",
                    error=f"Poll timeout after {elapsed:.0f}s (max {max_timeout}s)",
                    error_type="timeout",
                )

            try:
                response = httpx.get(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                status = PrintJobStatus(**response.json())

                if status.status == "completed":
                    logger.info(f"Job {job_id} completed in {elapsed:.0f}s")
                    return status

                if status.status == "failed":
                    logger.warning(
                        f"Job {job_id} failed: error_type={status.error_type}, "
                        f"error={status.error}"
                    )
                    return status

                # Still processing or queued
                logger.debug(
                    f"Job {job_id} status: {status.status}. "
                    f"Elapsed: {elapsed:.0f}s. Waiting {self.poll_interval}s..."
                )
                time.sleep(self.poll_interval)

            except httpx.HTTPStatusError as e:
                logger.error(f"Poll HTTP error for {job_id}: {e.response.status_code}")
                return PrintJobStatus(
                    id=job_id,
                    filename="",
                    status="failed",
                    error=f"HTTP {e.response.status_code} during polling",
                    error_type="unknown",
                )
            except httpx.RequestError as e:
                logger.error(f"Poll connection error for {job_id}: {e}")
                return PrintJobStatus(
                    id=job_id,
                    filename="",
                    status="failed",
                    error=f"Connection error during polling: {str(e)}",
                    error_type="unknown",
                )

    def check_status(self, job_id: str) -> PrintJobStatus | None:
        """Single status check without polling loop.

        Returns:
            PrintJobStatus or None on error
        """
        url = f"{self.base_url}/job-status/{job_id}"
        try:
            response = httpx.get(
                url,
                headers=self.headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return PrintJobStatus(**response.json())
        except Exception as e:
            logger.error(f"Status check failed for {job_id}: {e}")
            return None
