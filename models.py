"""
Pydantic models for Flatten Orchestrator API contracts
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# --- ERP API Models ---

class ERPDocument(BaseModel):
    """Document received from ERP GetDocsByDate endpoint"""
    lId: int
    jobId: int
    docPath: str
    fileName: str
    docType: int
    fileBase64: str
    # Additional fields from ERP that we pass through
    # The ERP may return more fields; we only model what we use


class SaveFlattenDocRequest(BaseModel):
    """Request body for ERP SaveFlattenDoc endpoint"""
    lId: int
    jobId: int
    docPath: str
    fileName: str
    docType: int  # This is the FLATTEN doc type, not the original
    flattenQueueId: str
    flattenStatusId: int  # 1 = success, 2 = failed
    fileBase64: str


# --- Print Queue API Models ---

class PrintQueueResponse(BaseModel):
    """Response from POST /print-queue"""
    job_id: str
    filename: str
    message: str
    status: str


class PrintJobStatus(BaseModel):
    """Response from GET /job-status/{job_id}"""
    id: str
    filename: str
    status: str  # queued, processing, completed, failed
    result: Optional[str] = None  # base64 of flattened PDF when completed
    output_path: Optional[str] = None
    output_filename: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None


# --- State Models ---

class TrackedJob(BaseModel):
    """A document being tracked through the flatten pipeline"""
    lId: int
    jobId: int
    docPath: str
    fileName: str
    original_docType: int
    flatten_docType: int
    input_file_path: Optional[str] = None   # Path to cached input PDF on disk
    output_file_path: Optional[str] = None  # Path to cached flattened PDF on disk
    print_queue_job_id: Optional[str] = None
    status: str  # fetched, submitted, polling, completed, saved_to_erp, failed, pending_retry, permanently_failed
    retry_count: int = 0
    error: Optional[str] = None
    error_type: Optional[str] = None
    submitted_at: Optional[str] = None
    completed_at: Optional[str] = None


class OrchestratorState(BaseModel):
    """Top-level state persisted to state.json"""
    last_fetch_timestamp: Optional[str] = None
    active_jobs: dict[str, TrackedJob] = {}
    completed_jobs: dict[str, dict] = {}
    failed_jobs: dict[str, dict] = {}
