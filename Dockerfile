###FROM docker.arvancloud.ir/library/python:3.12-slim
FROM nexus.sedrapro.com:8444/python:3.12-slim
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_INDEX_URL=https://package-mirror.liara.ir/repository/pypi/simple \
    PIP_TRUSTED_HOST=package-mirror.liara.ir

RUN addgroup --system --gid 1001 appuser && \
    adduser --system --uid 1001 --gid 1001 appuser

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        iputils-ping && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./

COPY src/ ./src

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 1220

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "1220"]