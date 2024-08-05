default: install lint check-types test test-integration

install:
    uv lock
    uv sync

lint:
    uv run -q --frozen ruff check .
    uv run -q --frozen ruff format .

check-types:
    uv run -q --frozen mypy .

test *args:
    uv run -q --frozen pytest {{args}}

test-integration *args:
    #!/bin/bash
    trap 'echo; docker compose down --remove-orphans' EXIT
    docker compose run --build --rm app .venv/bin/pytest tests/integration.py --no-cov {{args}}

run-artemis:
    docker compose run --service-ports artemis

run-consumer:
    ARTEMIS_HOST=0.0.0.0 uv run -q --frozen python testing/consumer.py

run-producer:
    ARTEMIS_HOST=0.0.0.0 uv run -q --frozen python testing/producer.py

publish:
    rm -rf dist/*
    uv tool run --from build python -m build --installer uv
    uv tool run twine check dist/*
    uv tool run twine upload dist/* --username __token__ --password $PYPI_TOKEN
