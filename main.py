"""
Flatten Orchestrator — Entry Point
Runs the fetch-submit-poll-save pipeline on a configurable interval.
"""
import time
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from config import config
from orchestrator import Orchestrator


def setup_logging():
    """Configure logging with console and daily-rotated file output"""
    config.create_dirs()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) 

    # Console handler (INFO level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler (DEBUG level, daily rotation)
    log_file = os.path.join(config.LOG_DIR, "orchestrator.log")
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)


def main():
    """Main entry point — runs the orchestrator on a scheduled loop"""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("FLATTEN ORCHESTRATOR STARTING")
    logger.info(f"Fetch interval: {config.FETCH_INTERVAL_MINUTES} minutes")
    logger.info(f"Poll interval: {config.POLL_INTERVAL_SECONDS} seconds")
    logger.info(f"Poll timeout: {config.MAX_POLL_TIMEOUT_SECONDS} seconds")
    logger.info(f"Batch size: {config.BATCH_SIZE}")
    logger.info(f"Max retries: {config.MAX_RETRIES}")
    logger.info(f"Retryable errors: {config.RETRYABLE_ERRORS}")
    logger.info(f"Supported doc types: {config.supported_doc_types()}")
    logger.info("=" * 60)

    orchestrator = Orchestrator()
    interval_seconds = config.FETCH_INTERVAL_MINUTES * 60


    cycle_count = 0

    while True:
        cycle_count += 1
        now = datetime.now()
        logger.info(f"--- CYCLE {cycle_count} at {now.isoformat()} ---")

        try:
            summary = orchestrator.run_cycle()
            logger.info(
                f"Cycle {cycle_count} summary: "
                f"fetched={summary['fetched']}, "
                f"submitted={summary['submitted']}, "
                f"completed={summary['completed']}, "
                f"saved={summary['saved_to_erp']}, "
                f"failed={summary['failed']}, "
                f"retried={summary['retried']}"
            )


        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Cycle {cycle_count} failed with unexpected error: {e}", exc_info=True)

        logger.info(f"Next cycle in {config.FETCH_INTERVAL_MINUTES} minutes...")

        try:
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Shutdown requested. Exiting.")
            break

    logger.info("FLATTEN ORCHESTRATOR STOPPED")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutdown complete.")
