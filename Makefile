.PHONY: format check fix test ci help

help:
	@echo "Available targets:"
	@echo "  check  - Check formatting and linting"
	@echo "  ci     - Run checks and tests (for CI)"
	@echo "  fix    - Fix linting issues automatically"
	@echo "  format - Format code with ruff"
	@echo "  test   - Run tests with pytest"

check:
	uv run ruff format --check .
	uv run ruff check .

ci: check test

fix:
	uv run ruff check --fix .

format:
	uv run ruff format .

test:
	uv run pytest
