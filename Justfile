default: install lint check-types test build-docker test-integration

install:
    uv -q lock
    uv -q sync

test *args:
    uv -q run pytest {{args}}

lint:
    uv -q run ruff check .
    uv -q run ruff format .

check-types:
    uv -q run mypy .

run-artemis:
    docker compose up

run-consumer:
    uv -q run python testing/consumer.py

run-producer:
    uv -q run python testing/producer.py

build-docker:
    docker buildx bake

test-integration *args:
    #!/bin/bash
    trap 'echo; docker compose down --remove-orphans' EXIT
    docker compose run --rm app .venv/bin/pytest tests/integration.py --no-cov {{args}}
