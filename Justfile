default: lint check-types test test-integration

lint:
    uv run ruff check .
    uv run ruff format .

check-types:
    uv run mypy .

test *args:
    uv run pytest {{args}}

test-integration *args:
    #!/bin/bash
    trap 'echo; docker compose down --remove-orphans' EXIT
    docker compose run --build --rm app .venv/bin/pytest tests/integration.py --no-cov {{args}}

run-artemis:
    #!/bin/bash
    trap 'echo; docker compose down --remove-orphans' EXIT
    docker compose run --service-ports artemis

run-consumer:
    uv run examples/consumer.py

run-producer:
    uv run examples/producer.py

publish:
    rm -rf dist
    uv build
    uv publish --token $PYPI_TOKEN
