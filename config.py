"""
Configuration for Flatten Orchestrator.
All secrets and configurable values are read from environment variables (.env file).
"""
import os
from dotenv import load_dotenv

# Load .env from the project root
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int = 0) -> int:
    return int(os.environ.get(key, default))


class Config:
    """Central configuration — all values sourced from environment"""

    # --- ERP API ---
    ERP_BASE_URL = _env("ERP_BASE_URL", "http://localhost:5056")
    ERP_EMAIL = _env("ERP_EMAIL", "it.ops@babajishivram.com")
    ERP_PASSWORD = _env("ERP_PASSWORD")

    # ERP API endpoints (relative to ERP_BASE_URL)
    ERP_LOGIN_ENDPOINT = "/api/Login"
    ERP_GET_DOCS_ENDPOINT = "/api/PreAlertDoc/GetDocsByDate"
    ERP_SAVE_FLATTEN_ENDPOINT = "/api/PreAlertDoc/SaveFlattenDoc"

    # --- Print Queue API ---
    PRINT_QUEUE_URL = _env("PRINT_QUEUE_URL", "http://127.0.0.1:8001")
    PRINT_API_KEY = _env("PRINT_API_KEY")



    # --- Scheduling ---
    FETCH_INTERVAL_MINUTES = _env_int("FETCH_INTERVAL_MINUTES", 15)
    POLL_INTERVAL_SECONDS = _env_int("POLL_INTERVAL_SECONDS", 30)
    MAX_POLL_TIMEOUT_SECONDS = _env_int("MAX_POLL_TIMEOUT_SECONDS", 90)
    HTTP_TIMEOUT_SECONDS = 30

    # --- Batch Processing ---
    BATCH_SIZE = _env_int("BATCH_SIZE", 10)

    # --- Retry ---
    MAX_RETRIES = _env_int("MAX_RETRIES", 3)
    RETRYABLE_ERRORS = ["timeout", "acrobat_not_found", "file_locked", "print_queue_unavailable"]

    # --- Paths ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STATES_DIR = os.path.join(BASE_DIR, "States")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    CACHE_DIR = os.path.join(BASE_DIR, "cache")

    # --- Document Type Mapping (normal docType -> flatten docType) ---
    DOC_TYPE_MAP = {
        104: 126,  # BOE Copy Flatten
        109: 127,  # Final OOC Copy Flatten
        110: 128,  # eGatepass Copy Flatten
        111: 129,  # Shipping Bill Copy Flatten
        112: 130,  # Shipping Final LEO Copy Flatten
        113: 131,  # Shipping eGatepass Copy Flatten
    }

    @classmethod
    def supported_doc_types(cls) -> list:
        return list(cls.DOC_TYPE_MAP.keys())

    @classmethod
    def get_flatten_doc_type(cls, normal_doc_type: int) -> int:
        return cls.DOC_TYPE_MAP[normal_doc_type]

    @classmethod
    def create_dirs(cls):
        os.makedirs(cls.LOG_DIR, exist_ok=True)
        os.makedirs(cls.CACHE_DIR, exist_ok=True)
        os.makedirs(cls.STATES_DIR, exist_ok=True)



config = Config()
