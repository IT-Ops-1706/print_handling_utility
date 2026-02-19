# Flatten Orchestrator

Middleware utility that orchestrates the PDF flattening pipeline between the ERP API and the Print Queue API.

## System Context

```
ERP API (PreAlertDoc)                Print Queue API (localhost:8001)
       |                                        |
       |  GET /GetDocsByDate                     |
       |  (fetch unprocessed docs)               |
       v                                        |
  +-----------------------------------------+   |
  |        FLATTEN ORCHESTRATOR             |   |
  |                                         |   |
  |  1. Fetch docs from ERP                 |   |
  |  2. Submit PDFs to Print Queue  ------->|   |
  |  3. Poll for completion (30s)   <-------|   |
  |  4. Send flattened PDF back to ERP      |   |
  |  5. Retry on failures                   |   |
  +-----------------------------------------+   |
       |                                        |
       |  POST /SaveFlattenDoc                   |
       v                                        |
    ERP API                                     |
  (stores flattened PDF)                        |
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure
# Edit config.py with API endpoints and credentials

# Run
python main.py
```

## Documentation

- [SRS.md](./docs/SRS.md) — Full Software Requirements Specification
