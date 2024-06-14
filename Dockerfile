ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bullseye

# hadolint ignore=DL3013, DL3042
RUN --mount=type=cache,target=~/.cache/pip pip install uv

WORKDIR /app
COPY pyproject.toml  .
RUN uv lock

COPY stompman/__init__.py stompman/__init__.py
COPY README.md .
RUN --mount=type=cache,target=~/.cache/uv uv sync

COPY . .
