FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    SERVERS_CONFIG_PATH=/etc/collect-metrics/config.yaml \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /uvx /bin/

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY collect-metrics.py .

EXPOSE 8000

CMD ["uv", "run", "collect-metrics.py"]
