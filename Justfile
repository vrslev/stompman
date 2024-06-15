default: install lint check-types test test-integration

install:
    uv -q lock
    uv -q sync

test *args:
    uv -q run pytest -- {{args}}

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

test-integration *args:
    #!/bin/bash
    trap 'echo; docker compose down --remove-orphans' EXIT
    docker compose run --build --rm app .venv/bin/pytest tests/integration.py --no-cov {{args}}

publish:
    rm -rf dist/*
    uv tool run --from build python -- -m build --installer uv
    uv tool run twine check dist/*
    uv tool run twine upload dist/* --username __token__ --password $PYPI_TOKEN
