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
    uv run testing/consumer.py

run-producer:
    uv run testing/producer.py

publish:
    rm -rf dist/*
    uvx --from build python -m build --installer uv
    uvx twine check dist/*
    uvx twine upload dist/* --username __token__ --password $PYPI_TOKEN
