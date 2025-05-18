FROM python:3.11.8-alpine3.20 as builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.11.8-alpine3.20

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY schema_registry_migrator.py .

RUN apk add --no-cache curl

ENV PYTHONUNBUFFERED=1

RUN adduser -D appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["python", "schema_registry_migrator.py"]
