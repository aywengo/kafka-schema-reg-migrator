FROM python:3.13-alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schema_registry_migrator.py .

ENV PYTHONUNBUFFERED=1

RUN adduser -D appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["python", "schema_registry_migrator.py"]
