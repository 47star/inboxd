.PHONY: check lint test format setup

check: lint test

setup:
	uv sync --locked --group dev
	git config --local core.hooksPath .githooks

lint:
	uv run --locked ruff check .
	uv run --locked ruff format --check .
	uv run --locked python scripts/check_layout.py
	xcrun swift-format lint --strict --recursive --configuration .swift-format native
	uv run --locked python scripts/check_native.py

test:
	uv run --locked pytest -q

format:
	uv run --locked ruff format .
	uv run --locked ruff check --fix .
	uv run --locked python scripts/check_layout.py --fix
	xcrun swift-format format --in-place --recursive --configuration .swift-format native
