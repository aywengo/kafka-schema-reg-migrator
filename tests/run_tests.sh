#!/bin/bash

# Parse command line arguments
DEBUG_MODE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --debug)
            DEBUG_MODE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--debug]"
            exit 1
            ;;
    esac
done

# Only exit on error if not in debug mode
if [ "$DEBUG_MODE" = false ]; then
    set -e
fi

# Function to wait for a service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_retries=60
    local retry_interval=5
    local retries=0

    echo "Waiting for service $service_name at $url to be ready..."
    while [ $retries -lt $max_retries ]; do
        if curl -s "$url" > /dev/null; then
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
    if [ "$DEBUG_MODE" = false ]; then
        echo "Cleaning up..."
        docker-compose down
    else
        echo "Debug mode: Keeping environment running"
        echo "To clean up manually, run: docker-compose down"
    fi
}

# Set up cleanup on script exit
trap cleanup EXIT

# Start the test environment
echo "Starting test environment..."
docker-compose up -d schema-registry-source schema-registry-dest schema-registry-ui-source schema-registry-ui-dest

# Wait for both schema registries to be ready
echo "Waiting for schema registries to be ready..."
if ! wait_for_service "http://localhost:38081/subjects" "schema-registry-source"; then
    echo "Source schema registry failed to start"
    docker-compose logs schema-registry-source
    if [ "$DEBUG_MODE" = false ]; then
        exit 1
    fi
fi

if ! wait_for_service "http://localhost:38082/subjects" "schema-registry-dest"; then
    echo "Destination schema registry failed to start"
    docker-compose logs schema-registry-dest
    if [ "$DEBUG_MODE" = false ]; then
        exit 1
    fi
fi

# Wait for Schema Registry UIs to be ready
echo "Waiting for Schema Registry UIs to be ready..."
if ! wait_for_service "http://localhost:38091" "schema-registry-ui-source"; then
    echo "Source Schema Registry UI failed to start"
    docker-compose logs schema-registry-ui-source
    if [ "$DEBUG_MODE" = false ]; then
        exit 1
    fi
fi

if ! wait_for_service "http://localhost:38092" "schema-registry-ui-dest"; then
    echo "Destination Schema Registry UI failed to start"
    docker-compose logs schema-registry-ui-dest
    if [ "$DEBUG_MODE" = false ]; then
        exit 1
    fi
fi

# Additional wait to ensure Kafka is fully ready
echo "Waiting for Kafka to be fully ready..."
sleep 5

# Clean up destination registry before starting tests
echo "Cleaning up destination registry..."
curl -X DELETE http://localhost:38082/subjects/* || true

# Wait for cleanup to complete
echo "Waiting for destination registry cleanup to complete..."
max_retries=30
retry_interval=2
for i in $(seq 1 $max_retries); do
    subjects=$(curl -s http://localhost:38082/subjects)
    if [ "$subjects" = "[]" ]; then
        echo "Destination registry cleanup completed"
        break
    fi
    if [ $i -eq $max_retries ]; then
        echo "Destination registry cleanup failed"
        if [ "$DEBUG_MODE" = false ]; then
            exit 1
        fi
    fi
    echo "Waiting for destination registry cleanup... ($i/$max_retries)"
    sleep $retry_interval
done

# Populate source schema registry
echo "Populating source schema registry..."
python populate_source.py

# Wait for source registry population to complete
echo "Waiting for source registry population to complete..."
for i in $(seq 1 $max_retries); do
    subjects=$(curl -s http://localhost:38081/subjects)
    if echo "$subjects" | grep -q "test-value"; then
        echo "Source registry population completed"
        break
    fi
    if [ $i -eq $max_retries ]; then
        echo "Source registry population failed"
        if [ "$DEBUG_MODE" = false ]; then
            exit 1
        fi
    fi
    echo "Waiting for source registry population... ($i/$max_retries)"
    sleep $retry_interval
done

# Run migration tests
echo "Running migration tests..."
if python test_migration.py; then
    echo "All tests completed successfully!"
else
    if [ "$DEBUG_MODE" = true ]; then
        echo "Tests failed but debug mode is enabled. Environment is still running."
        echo "You can inspect the environment and run tests manually."
        echo "To clean up, run: docker-compose down"
        exit 1
    else
        echo "Tests failed"
        exit 1
    fi
fi 