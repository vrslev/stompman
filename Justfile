default: install lint check-types test test-integration

install:
    poetry install --sync

test *args:
    poetry run pytest {{args}}

lint:
    poetry run ruff check .
    poetry run ruff format .

check-types:
    poetry run mypy .

run-artemis:
    docker compose up

run-consumer:
    poetry run python testing/consumer.py

run-producer:
    poetry run python testing/producer.py

test-integration *args:
    docker compose down --remove-orphans
    docker compose run --build --rm app poetry run pytest tests/integration.py --no-cov {{args}}
    docker compose down --remove-orphans
