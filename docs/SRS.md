# Software Requirements Specification
# Flatten Orchestrator — Middleware Utility

**Version**: 1.0  
**Date**: 2026-02-19  
**Status**: DRAFT  

---

## 1. Purpose

The Flatten Orchestrator is a Python-based middleware utility that automates the end-to-end PDF flattening workflow. It bridges the ERP system (PreAlertDoc API) and the Print Queue API (Adobe Acrobat automation) to ensure digital signatures in customs/shipping documents are rendered as static content (green tick) before being sent to end users.

### 1.1 Problem Statement

Documents downloaded from Indian Customs (ICEGATE) contain live digital signatures. When these PDFs are merged and sent to users without flattening, the signature field shows a "Signature Not Verified" question mark instead of a valid green tick. Flattening via Adobe Acrobat's print-to-PDF resolves this.

### 1.2 Scope

This utility:
- Fetches unprocessed documents from the ERP API
- Submits them to the Print Queue for flattening
- Polls for completion
- Maps document types to their flattened equivalents
- Sends flattened PDFs back to the ERP API
- Handles retries and failure tracking

This utility does **NOT**:
- Perform the actual PDF flattening (that is the Print Queue's job)
- Merge documents (that is handled by the Pdf Merge Utility)
- Handle digital signing (that is handled by the Digital Sign utility)

---

## 2. System Architecture

```
+------------------+       +------------------------+       +------------------+
|                  |       |                        |       |                  |
|    ERP API       | <---> |  FLATTEN ORCHESTRATOR   | <---> |  PRINT QUEUE API |
|  (PreAlertDoc)   |       |                        |       |  (localhost:8001)|
|                  |       |                        |       |                  |
+------------------+       +------------------------+       +------------------+
                                     |
                                     v
                              +-------------+
                              | state.json  |
                              | (tracking)  |
                              +-------------+
```

### 2.1 Dependencies

| System | Role | Endpoint |
|---|---|---|
| ERP API (PreAlertDoc) | Source of documents and destination for flattened PDFs | `http://localhost:5056/api/PreAlertDoc/` |
| Print Queue API | Flattens PDFs via Adobe Acrobat automation | `http://127.0.0.1:8001/` |

### 2.2 Component: Flatten Orchestrator

| Module | Responsibility |
|---|---|
| `main.py` | Entry point, scheduler loop (15-minute intervals) |
| `config.py` | All configuration: API endpoints, credentials, intervals, doc type mappings |
| `erp_client.py` | HTTP client for ERP API (fetch docs, save flattened docs) |
| `print_queue_client.py` | HTTP client for Print Queue API (submit jobs, poll status) |
| `orchestrator.py` | Core business logic: fetch → submit → poll → save pipeline |
| `state_manager.py` | JSON-based state persistence (tracking, timestamps, retry state) |
| `models.py` | Pydantic models for API contracts |

---

## 3. Functional Requirements

### 3.1 Document Fetching

**FR-01**: The system SHALL fetch documents from the ERP API using:
```
GET /api/PreAlertDoc/GetDocsByDate?fromDate={timestamp}
Authorization: Bearer {token}
```

**FR-02**: On first run, `fromDate` SHALL be configurable (default: current date).

**FR-03**: On subsequent runs, `fromDate` SHALL be the timestamp of the last successful fetch, stored in `state.json`.

**FR-04**: The system SHALL only process documents whose `docType` is in the supported mapping table (see Section 3.5).

### 3.2 Job Submission

**FR-05**: For each fetched document, the system SHALL submit the PDF to the Print Queue API:
```
POST /print-queue
Authorization: Bearer {print_api_key}
Content-Type: multipart/form-data
file: {pdf_binary}
```

**FR-06**: The system SHALL store the returned `job_id` in `state.json` mapped to the document's ERP metadata (`lId`, `jobId`, `docPath`, `docType`).

**FR-07**: The system SHALL submit jobs sequentially (one at a time) to avoid overwhelming the Print Queue, which processes jobs sequentially.

### 3.3 Status Polling

**FR-08**: After submitting a job, the system SHALL poll the Print Queue every 30 seconds:
```
GET /job-status/{job_id}
Authorization: Bearer {print_api_key}
```

**FR-09**: Polling SHALL only check the **latest submitted job** (the one currently in `processing` state), not all jobs.

**FR-10**: Polling SHALL continue until the job status is `completed` or `failed`.

**FR-11**: If the job status is `completed`, the system SHALL retrieve the flattened PDF (base64) from the response.

**FR-12**: If the job status is `failed`, the system SHALL record the `error` and `error_type` in `state.json` and move to the next document.

### 3.4 Flattened Document Submission

**FR-13**: On successful flattening, the system SHALL submit the flattened PDF to the ERP API:
```json
POST /api/PreAlertDoc/SaveFlattenDoc
Authorization: Bearer {erp_token}
Content-Type: application/json

{
  "lId": <from_original_doc>,
  "jobId": <from_original_doc>,
  "docPath": "<from_original_doc>",
  "fileName": "<original_filename>_flatten.pdf",
  "docType": <mapped_flatten_doctype>,
  "flattenQueueId": "<print_queue_job_id>",
  "flattenStatusId": 1,
  "fileBase64": "<flattened_pdf_base64>"
}
```

**FR-14**: The `docType` in the submission SHALL be the **flatten equivalent**, not the original doc type (see mapping in Section 3.5).

**FR-15**: The `flattenQueueId` SHALL be the `job_id` returned by the Print Queue API.

**FR-16**: `flattenStatusId` SHALL be:
- `1` = Successfully flattened
- `2` = Failed (with error details)

### 3.5 Document Type Mapping

| Normal docType (from ERP) | Flatten docType (to ERP) | Document Name |
|---|---|---|
| 104 | 126 | BOE Copy Flatten |
| 109 | 127 | Final OOC Copy Flatten |
| 110 | 128 | eGatepass Copy Flatten |
| 111 | 129 | Shipping Bill Copy Flatten |
| 112 | 130 | Shipping Final LEO Copy Flatten |
| 113 | 131 | Shipping eGatepass Copy Flatten |

**FR-17**: Only documents with `docType` in `[104, 109, 110, 111, 112, 113]` SHALL be processed.

**FR-18**: The mapping SHALL be configurable in `config.py`.

### 3.6 Scheduling

**FR-19**: The system SHALL run the fetch-submit-poll-save cycle every **15 minutes**.

**FR-20**: The interval SHALL be configurable in `config.py`.

**FR-21**: Each cycle SHALL:
1. Fetch new documents since last timestamp
2. For each document: submit → poll → save
3. Update the last fetch timestamp in `state.json`

### 3.7 Error Handling and Retry

**FR-22**: The system SHALL classify errors based on the Print Queue's `error_type`:

| error_type | Action |
|---|---|
| `timeout` | Auto-retry (up to `MAX_RETRIES`) |
| `acrobat_not_found` | Auto-retry (Acrobat may need restart) |
| `ui_automation_failed` | Log for manual intervention, do NOT auto-retry |
| `file_validation_failed` | Log and skip (bad input) |
| `file_locked` | Auto-retry |
| `unknown` | Log for manual intervention |

**FR-23**: The system SHALL maintain a retry counter per document in `state.json`. Maximum retries SHALL be configurable (default: 3).

**FR-24**: Failed documents that exceed `MAX_RETRIES` SHALL be marked as `permanently_failed` in `state.json`.

**FR-25**: On each cycle, the system SHALL re-attempt documents that are `pending_retry` before fetching new ones.

---

## 4. State Management (`state.json`)

### 4.1 Schema

```json
{
  "last_fetch_timestamp": "2026-02-19T12:00:00",
  "active_jobs": {
    "doc_2391549": {
      "lId": 2391549,
      "jobId": 649427,
      "docPath": "WEST_8sZ3\\CB15763MBOI2425\\",
      "fileName": "BOE1.pdf",
      "original_docType": 104,
      "flatten_docType": 126,
      "print_queue_job_id": "job_1_9e80edb6",
      "status": "polling",
      "retry_count": 0,
      "error": null,
      "error_type": null,
      "submitted_at": "2026-02-19T12:07:07",
      "completed_at": null
    }
  },
  "completed_jobs": {
    "doc_2391548": {
      "lId": 2391548,
      "print_queue_job_id": "job_3_abc12345",
      "status": "saved_to_erp",
      "completed_at": "2026-02-19T11:55:00"
    }
  },
  "failed_jobs": {
    "doc_2391547": {
      "lId": 2391547,
      "error": "UI automation failed",
      "error_type": "ui_automation_failed",
      "retry_count": 3,
      "status": "permanently_failed"
    }
  }
}
```

### 4.2 State Transitions

```
FETCHED --> SUBMITTED --> POLLING --> COMPLETED --> SAVED_TO_ERP
                |             |
                |             +--> FAILED --> PENDING_RETRY --> SUBMITTED (loop)
                |                                |
                |                                +--> PERMANENTLY_FAILED
                |
                +--> SUBMIT_FAILED --> PENDING_RETRY --> SUBMITTED (loop)
```

### 4.3 Persistence

- Written atomically via temp file + `os.replace()` on every state transition.
- Loaded on startup. Any `POLLING` state jobs are treated as interrupted and re-polled.

---

## 5. Non-Functional Requirements

### 5.1 Reliability

- **NFR-01**: The system SHALL survive process restarts without data loss via `state.json`.
- **NFR-02**: The system SHALL not submit duplicate jobs for the same document (deduplicated by `lId`).
- **NFR-03**: All HTTP calls SHALL have configurable timeouts (default: 30 seconds).

### 5.2 Observability

- **NFR-04**: The system SHALL log every state transition with timestamps.
- **NFR-05**: Each cycle SHALL produce a summary log: documents fetched, submitted, completed, failed.
- **NFR-06**: Log files SHALL be stored in a `logs/` directory with daily rotation.

### 5.3 Performance

- **NFR-07**: The system SHALL process documents sequentially (one at a time through the Print Queue).
- **NFR-08**: Polling interval: 30 seconds per job.
- **NFR-09**: Fetch interval: 15 minutes between cycles.

### 5.4 Security

- **NFR-10**: API tokens SHALL be stored in `config.py` (internal utility, no env required).
- **NFR-11**: All HTTP communication is internal (localhost). No external network calls.

---

## 6. Project Structure

```
Flatten Orchestrator/
├── main.py                  # Entry point and scheduler
├── config.py                # Configuration (endpoints, tokens, intervals, mappings)
├── orchestrator.py          # Core pipeline logic
├── erp_client.py            # ERP API HTTP client
├── print_queue_client.py    # Print Queue API HTTP client
├── state_manager.py         # JSON state persistence
├── models.py                # Pydantic models
├── requirements.txt         # Dependencies
├── docs/
│   └── SRS.md               # This document
├── logs/                    # Runtime logs
└── state.json               # Runtime state (generated)
```

---

## 7. Configuration Reference

```python
# config.py outline

class Config:
    # ERP API
    ERP_BASE_URL = "http://localhost:5056/api/PreAlertDoc"
    ERP_TOKEN = "Bearer eyJ..."

    # Print Queue API
    PRINT_QUEUE_URL = "http://127.0.0.1:8001"
    PRINT_API_KEY = "BabajiShivram@1706"

    # Scheduling
    FETCH_INTERVAL_MINUTES = 15
    POLL_INTERVAL_SECONDS = 30
    HTTP_TIMEOUT_SECONDS = 30

    # Retry
    MAX_RETRIES = 3
    RETRYABLE_ERRORS = ["timeout", "acrobat_not_found", "file_locked"]

    # Document type mapping (normal -> flatten)
    DOC_TYPE_MAP = {
        104: 126,  # BOE Copy Flatten
        109: 127,  # Final OOC Copy Flatten
        110: 128,  # eGatepass Copy Flatten
        111: 129,  # Shipping Bill Copy Flatten
        112: 130,  # Shipping Final LEO Copy Flatten
        113: 131,  # Shipping eGatepass Copy Flatten
    }
```

---

## 8. Pipeline Flow (Pseudocode)

```
EVERY 15 MINUTES:
    1. Load state.json

    2. RETRY PHASE:
       For each job in state where status = "pending_retry":
           Re-submit to Print Queue
           Poll for completion (every 30s)
           If completed:
               Map docType -> flatten docType
               POST to ERP SaveFlattenDoc
               Move to completed_jobs
           If failed:
               Increment retry_count
               If retry_count >= MAX_RETRIES -> permanently_failed
               Else if error_type in RETRYABLE_ERRORS -> pending_retry
               Else -> permanently_failed

    3. FETCH PHASE:
       GET /GetDocsByDate?fromDate={last_fetch_timestamp}
       Filter: only docTypes in DOC_TYPE_MAP
       Deduplicate: skip any lId already in state

    4. PROCESS PHASE:
       For each new document:
           Submit PDF to Print Queue (POST /print-queue)
           Store job_id in state
           Poll for completion (every 30s)
           If completed:
               Map docType -> flatten docType
               POST to ERP SaveFlattenDoc
               Move to completed_jobs
           If failed:
               Same retry logic as step 2

    5. Update last_fetch_timestamp
    6. Save state.json
    7. Log cycle summary
```

---

## 9. API Contracts

### 9.1 ERP API — Fetch Documents

```
GET /api/PreAlertDoc/GetDocsByDate?fromDate=2026-02-10
Authorization: Bearer {token}
Accept: */*

Response: [
  {
    "lId": 2391549,
    "jobId": 649427,
    "docPath": "WEST_8sZ3\\CB15763MBOI2425\\",
    "fileName": "BOE1.pdf",
    "docType": 104,
    "fileBase64": "<base64_encoded_pdf>",
    ... (other metadata)
  }
]
```

### 9.2 Print Queue API — Submit Job

```
POST /print-queue
Authorization: Bearer {print_api_key}
Content-Type: multipart/form-data
file: <pdf_binary>

Response: {
  "job_id": "job_1_9e80edb6",
  "filename": "BOE1.pdf",
  "message": "Print job queued successfully",
  "status": "queued"
}
```

### 9.3 Print Queue API — Poll Status

```
GET /job-status/{job_id}
Authorization: Bearer {print_api_key}

Response (completed): {
  "id": "job_1_9e80edb6",
  "filename": "BOE1.pdf",
  "status": "completed",
  "result": "<base64_flattened_pdf>",
  "output_path": "...",
  "error": null,
  "error_type": null
}

Response (failed): {
  "id": "job_1_9e80edb6",
  "status": "failed",
  "error": "Save As PDF dialog did not appear within 30 seconds.",
  "error_type": "timeout"
}
```

### 9.4 ERP API — Save Flattened Document

```
POST /api/PreAlertDoc/SaveFlattenDoc
Authorization: Bearer {token}
Content-Type: application/json

{
  "lId": 2391549,
  "jobId": 649427,
  "docPath": "WEST_8sZ3\\CB15763MBOI2425\\",
  "fileName": "BOE1_flatten.pdf",
  "docType": 126,
  "flattenQueueId": "job_1_9e80edb6",
  "flattenStatusId": 1,
  "fileBase64": "<base64_flattened_pdf>"
}
```

---

## 10. Acceptance Criteria

| # | Criteria |
|---|---|
| AC-01 | Utility fetches documents from ERP and submits them to Print Queue |
| AC-02 | Polling correctly waits for job completion at 30-second intervals |
| AC-03 | Flattened PDF is sent back to ERP with correct flatten `docType` mapping |
| AC-04 | `flattenQueueId` matches the Print Queue `job_id` |
| AC-05 | State survives process restart via `state.json` |
| AC-06 | Retryable errors are automatically retried up to `MAX_RETRIES` |
| AC-07 | Non-retryable errors (e.g. `ui_automation_failed`) are logged for manual review |
| AC-08 | Duplicate documents are not re-submitted (deduplication by `lId`) |
| AC-09 | 15-minute cycle runs continuously without memory leaks |
| AC-10 | All state transitions are logged with timestamps |
