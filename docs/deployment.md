# Deployment Guide

## Local / Bare-Metal Execution

```bash
uv run stackpulse serve --host 0.0.0.0 --port 8098
```

## Docker & Docker Compose

```bash
docker compose up --build -d
curl -s http://localhost:8098/api/v1/health
```
