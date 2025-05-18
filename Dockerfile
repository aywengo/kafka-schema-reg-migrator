FROM python:3.9-slim-bullseye

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip

# Copy application code
COPY schema_registry_migrator.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Default command for running the application
ENTRYPOINT ["python", "schema_registry_migrator.py"]
