FROM python:3.11-alpine as builder

WORKDIR /app
COPY requirements.txt .

# Install build dependencies
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev \
    cargo \
    curl \
    librdkafka-dev

# Install confluent-kafka wheel
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir wheel && \
    pip install --no-cache-dir confluent-kafka==2.3.0 && \
    pip install --no-cache-dir -r requirements.txt

FROM python:3.11-alpine

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY schema_registry_migrator.py .

RUN apk add --no-cache curl librdkafka

ENV PYTHONUNBUFFERED=1

RUN adduser -D appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["python", "schema_registry_migrator.py"]
