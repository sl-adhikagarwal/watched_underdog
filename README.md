# Intelligent Cloud Storage System

An end-to-end reference implementation for policy-driven, ML-assisted tiering across hot, warm, and cold storage. The platform combines FastAPI, Kafka, MinIO, and Streamlit to simulate a control plane that can ingest access patterns, predict optimal placement, trigger movements, and visualize the health of the estate.

## Overview

- **Predictive tiering** - A Random Forest model (with rule-based fallback) recommends tier changes using recent access frequency, latency, and data size.
- **Coordinated moves** - Decisions publish to Kafka, a mover service executes copy/verify flows against MinIO, and status updates flow back to the API.
- **Operational telemetry** - Metrics capture tier distribution, cost estimates, and movement success rates for dashboard consumption.

## Architecture at a Glance

| Component  | Technology           | Responsibility                                      |
|------------|----------------------|-----------------------------------------------------|
| Backend    | FastAPI, SQLAlchemy  | API surface, business logic, metrics persistence    |
| ML Engine  | scikit-learn         | Tier predictions + metadata persistence             |
| Producer   | Kafka, Requests      | Synthetic access load and auto decision triggers    |
| Mover      | Kafka consumer, MinIO| Executes copy/verify/delete flows between buckets   |
| Dashboard  | Streamlit, Plotly    | Observability and operator controls                 |
| Storage    | MinIO                | Hot / warm / cold buckets used by the mover         |

## Getting Started

1. **Install dependencies** - Docker and Docker Compose v2 or later are required. Local Python installs are optional because the stack runs in containers.
2. **Configure environment** - Copy `.env.example` to `.env` (or reuse the provided defaults for Kafka and MinIO credentials).
3. **Launch the stack**  
   ```bash
   docker compose up --build
   ```
4. **Access the services**
   - API docs: http://localhost:8000/docs  
   - Dashboard: http://localhost:8501  
   - MinIO console: http://localhost:9001

## Operations Guide

- **Retrain the ML model**  
  ```bash
  curl -X POST http://localhost:8000/ml/retrain
  ```
- **Simulate access traffic** - The `producer` container is enabled by default. Stop or start it independently via `docker compose stop producer` / `docker compose start producer`.
- **Inspect movement history**  
  ```bash
  curl http://localhost:8000/movements
  ```
- **Apply database migrations manually**  
  ```bash
  python backend/migrations.py
  ```
- **Lint and format** - Use `ruff` or `black` if desired; both can be added via `pip install -r dev-requirements.txt` (not included by default).

## Open Questions & Next Steps

1. **Schema management** - Should we formalize Alembic migrations so legacy SQLite volumes are not bricked when new columns (e.g., `access_window_start`) land?  
2. **Metric cadence** - Does `/metrics` need a background worker or materialized view so history is collected on a cadence instead of user traffic?  
3. **Cost modeling** - Are the flat per-tier cost constants sufficient, or should they be parameterized per dataset or per tenant?  
4. **Movement safety** - Do we need a two-phase commit (lock + verify) before updating dataset tiers to handle concurrent writes?  
5. **Access governance** - How granular should future access controls be (per-tenant API tokens, RBAC, or org-level quotas)?

## License

This project is released under the MIT License. See `LICENSE` for details.
