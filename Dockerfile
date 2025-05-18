FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY schema_registry_migrator.py .
COPY tests/ tests/

# Make test scripts executable
RUN chmod +x tests/run_tests.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command for running the application
ENTRYPOINT ["python", "schema_registry_migrator.py"]

# Command for running tests
CMD ["bash", "-c", "cd tests && ./run_tests.sh"] 