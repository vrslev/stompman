default: install lint check-types test test-integration

install:
    uv lock
    uv sync

test *args:
    uv run pytest {{args}}

lint:
    uv run ruff check .
    uv run ruff format .

check-types:
    uv run mypy .

run-artemis:
    docker compose up

run-consumer:
    uv run python testing/consumer.py

run-producer:
    uv run python testing/producer.py

test-integration *args:
    docker compose down --remove-orphans
    docker compose run --build --rm app uv run pytest tests/integration.py --no-cov {{args}}
    docker compose down --remove-orphans
