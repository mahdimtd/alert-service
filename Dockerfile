FROM docker.arvancloud.ir/library/python:3.12-slim
WORKDIR /src

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/src
ENV PIP_INDEX_URL=https://mirror-pypi.runflare.com/simple
ENV PIP_TRUSTED_HOST=mirror-pypi.runflare.com

ENV OTEL_SERVICE_NAME="homeiot-alert-service" \
    OTEL_EXPORTER_OTLP_ENDPOINT="http://192.168.140.41:4317" \
    OTEL_EXPORTER_OTLP_PROTOCOL="grpc" \
    OTEL_TRACES_EXPORTER="otlp" \
    OTEL_METRICS_EXPORTER="none" \
    OTEL_LOGS_EXPORTER="none"

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
RUN uv pip install --system --no-cache -r pyproject.toml

COPY src/ /src

RUN pip install --no-cache-dir \
        opentelemetry-distro \
        opentelemetry-exporter-otlp \
    && opentelemetry-bootstrap -a install 


EXPOSE 1220

CMD ["opentelemetry-instrument", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "1220"]
