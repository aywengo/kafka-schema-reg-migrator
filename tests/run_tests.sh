#!/bin/bash

set -e

# Function to wait for a service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_retries=60
    local retry_interval=5
    local retries=0

    echo "Waiting for service $service_name at $url to be ready..."
    while [ $retries -lt $max_retries ]; do
        if curl -s "$url/subjects" > /dev/null; then
            echo "Service $service_name at $url is ready!"
            return 0
        fi
        echo "Service $service_name not ready yet... ($((retries + 1))/$max_retries)"
        sleep $retry_interval
        retries=$((retries + 1))
    done
    echo "Service $service_name at $url failed to become ready after $max_retries attempts"
    return 1
}

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up..."
    docker-compose down
}

# Set up cleanup on script exit
trap cleanup EXIT

# Start the test environment
echo "Starting test environment..."
docker-compose up -d schema-registry-source schema-registry-dest

# Wait for both schema registries to be ready
echo "Waiting for schema registries to be ready..."
if ! wait_for_service "http://localhost:38081" "schema-registry-source"; then
    echo "Source schema registry failed to start"
    docker-compose logs schema-registry-source
    exit 1
fi

if ! wait_for_service "http://localhost:38082" "schema-registry-dest"; then
    echo "Destination schema registry failed to start"
    docker-compose logs schema-registry-dest
    exit 1
fi

# Additional wait to ensure Kafka is fully ready
echo "Waiting for Kafka to be fully ready..."
sleep 5

# Populate source schema registry
echo "Populating source schema registry..."
python populate_source.py

# Run migration tests
echo "Running migration tests..."
python test_migration.py

echo "All tests completed successfully!" 