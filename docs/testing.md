# Testing & Verification Guide

## Running the Test Suite

StackPulse includes 47 automated tests in `tests/test_stackpulse.py` covering ingestion, DAG critical-path calculation, overlapping async child intervals, N+1 detection, retry storm detection, service topology graphs, multi-window SLO math, incident postmortem correlation, CLI execution, and FastAPI endpoints.

```bash
uv run --with pytest --with fastapi --with httpx --with pydantic --with uvicorn pytest -v
```

## Running the Pipeline Benchmark

```bash
uv run stackpulse benchmark --traces 500
```
