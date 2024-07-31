ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-bullseye

# hadolint ignore=DL3013,DL3042
RUN pip install uv

WORKDIR /app
COPY pyproject.toml README.md ./
COPY stompman/__init__.py stompman/__init__.py

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0
RUN --mount=type=cache,target=~/.cache/uv \
    uv lock && uv sync
COPY . .
