ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bullseye

# hadolint ignore=DL3013, DL3042
RUN pip install poetry
COPY pyproject.toml ./
RUN poetry install
COPY . .
