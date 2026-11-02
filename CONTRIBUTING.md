# Contributing to StackPulse

Contributions improving OpenTelemetry trace analysis, SLO burn-rate math, or topology anomaly detection are welcome.

## Development Workflow

1. Clone the repository and run the test suite via `uv`:
   ```bash
   uv run --with pytest --with fastapi --with httpx --with pydantic --with uvicorn pytest -v
   ```
2. Run code style checks with Ruff:
   ```bash
   uv run --with ruff ruff check .
   ```
3. Ensure every new analyzer heuristic or API route includes deterministic unit tests in `tests/test_stackpulse.py`.
