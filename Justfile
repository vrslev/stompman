default: install lint-format check-types test

install:
    poetry install --sync

test *args:
    poetry run pytest {{args}}

lint-format:
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

publish number:
    poetry version {{number}}
    poetry publish --build

test-integration *args:
    docker compose down --remove-orphans
    docker compose up -d
    @just test tests/integration.py --no-cov {{args}}
    docker compose down --remove-orphans
