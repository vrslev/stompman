ARG PYTHON_IMAGE
# hadolint ignore=DL3006
FROM ${PYTHON_IMAGE}

# hadolint ignore=DL3013,DL3042
RUN pip install uv

WORKDIR /app
COPY pyproject.toml README.md ./
COPY packages/stompman/stompman/__init__.py packages/stompman/stompman/__init__.py
COPY packages/stompman/pyproject.toml packages/stompman/pyproject.toml
COPY packages/faststream-stomp/README.md packages/faststream-stomp/README.md
COPY packages/faststream-stomp/pyproject.toml packages/faststream-stomp/pyproject.toml
COPY packages/faststream-stomp/faststream_stomp/__init__.py packages/faststream-stomp/faststream_stomp/__init__.py

ENV SETUPTOOLS_SCM_PRETEND_VERSION=0
RUN --mount=type=cache,target=~/.cache/uv \
    ls packages/faststream-stomp && uv lock && uv sync
COPY . .
