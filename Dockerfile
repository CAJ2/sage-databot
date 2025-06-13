# Base image with Python and uv
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev
WORKDIR /app
ENV UV_SYSTEM_PYTHON=1
ENV PATH="/root/.local/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

COPY src/ pyproject.toml uv.lock prefect.yaml .python-version /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked
ENV PATH="/app/.venv/bin:$PATH"

FROM base AS server
CMD ["prefect", "server", "start"]
