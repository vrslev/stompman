ARG PYTHON_VERSION
FROM ghcr.io/astral-sh/uv:latest as uv
FROM python:${PYTHON_VERSION}-slim-bullseye

WORKDIR /app
COPY pyproject.toml README.md ./
COPY stompman/__init__.py stompman/__init__.py

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0
RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=~/.cache/uv \
    uv lock && uv sync
COPY . .
