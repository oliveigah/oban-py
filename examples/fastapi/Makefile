.PHONY: help setup db-create db-drop db-reset install uninstall dev worker clean

# Database name from oban.toml
DB_NAME := oban_py_fastapi_demo

help:
	@echo "FastAPI + Oban Demo - Available commands:"
	@echo ""
	@echo "  make setup      - Complete setup (create db, install deps, install schema)"
	@echo "  make install    - Install Oban schema into database"
	@echo "  make uninstall  - Uninstall Oban schema from database"
	@echo "  make dev        - Run FastAPI app with auto-reload"
	@echo "  make worker     - Run Oban worker process"
	@echo ""

setup: db-create install
	@echo "✓ Setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run workers:  make worker"
	@echo "  2. Run FastAPI:  make dev"
	@echo ""

db-create:
	@echo "Creating database $(DB_NAME)..."
	@createdb $(DB_NAME) 2>/dev/null || echo "Database $(DB_NAME) already exists"
	@uv sync

db-drop:
	@echo "Dropping database $(DB_NAME)..."
	@dropdb $(DB_NAME) 2>/dev/null || echo "Database $(DB_NAME) does not exist"

db-reset: db-drop db-create install
	@echo "✓ Database reset complete"

install:
	@echo "Installing Oban schema..."
	@uv run oban install

uninstall:
	@echo "Uninstalling Oban schema..."
	@uv run oban uninstall

dev:
	@echo "Starting FastAPI application..."
	@uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

worker:
	@echo "Starting Oban worker..."
	@PYTHONPATH=. uv run oban start
