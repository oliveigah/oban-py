# FastAPI + Oban Demo

This example demonstrates how to integrate Oban with a FastAPI application for asynchronous job
processing.

This demo shows:

- **FastAPI as a job client**: The web application enqueues jobs but doesn't process them
- **Oban CLI as a job processor**: A separate process handles job execution
- **Multiple queues**: Jobs are distributed across `mailers`, `reports`, and `default` queues
- **Cron jobs**: Periodic tasks that run on a schedule
- **Shared configuration**: Both the FastAPI app and Oban CLI use `oban.toml`

## Requirements

- Python 3.12+
- PostgreSQL 14+
- [uv](https://github.com/astral-sh/uv) package manager

## Installation

Use the included Makefile to automate the entire setup:

```bash
cd examples/fastapi
make setup
```

That will:

1. Create the PostgreSQL database
2. Install Python dependencies with `uv sync`
3. Create the Oban database schema

## Running the Application

You'll need to run **two processes** in separate terminals:

#### Terminal 1: Start Oban

```bash
make worker
```

This starts an Oban worker process that will process jobs and run scheduled cron jobs.

#### Terminal 2: Start FastAPI

```bash
make dev
```

The FastAPI app will be available at `http://localhost:8000`.

## Usage

To enqueue an email job:

```bash
curl -X POST "http://localhost:8000/send-email?to=user@example.com&subject=Hello&body=Welcome"
```

To enqueue a report:

```bash
curl -X POST "http://localhost:8000/generate-report?report_type=monthly_sales&user_id=42"
```

Both endpoints simply return the job ID:

```json
{"job_id": 1}
```

Watch the Oban terminal to see jobs being "processed" (this is a demo, they don't actually do
anything).

## Cleanup

To uninstall the Oban schema and drop all tables:

```bash
make uninstall
```

## Configuration

The `oban.toml` file contains shared configuration for both the FastAPI app and Oban CLI. That
ensures the Oban client stays in sync with the Oban server process.

```toml
# Database connection
dsn = "postgresql://localhost/oban_py_fastapi_demo"

# Queue configurations (name = concurrency limit)
[queues]
default = 10  # Health checks and general tasks
mailers = 5   # Email sending jobs
reports = 3   # Report generation jobs
```
