ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bullseye

# hadolint ignore=DL3013, DL3042
RUN --mount=type=cache,target=~/.cache/pip pip install uv

WORKDIR /app

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0
COPY pyproject.toml README.md ./
RUN uv lock

COPY stompman/__init__.py stompman/__init__.py
RUN --mount=type=cache,target=~/.cache/uv uv sync

COPY . .
