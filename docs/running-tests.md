# Running Tests

This document describes how to run the test suite for the Kafka Schema Registry Migrator.

## Prerequisites

- Docker
- Docker Compose (v2 recommended)
- Python 3.8+ (for local testing)

## Test Environment

The test environment consists of:
- Two independent Kafka clusters (source and destination)
- Two Schema Registry instances
- Test schemas with different versions
- Automated test scripts

## Running Tests Locally

### 1. Make Test Script Executable

```bash
chmod +x tests/run_tests.sh
```

### 2. Run Tests

```bash
cd tests
./run_tests.sh
```

## Running Tests in Docker

### 1. Build the Docker Image

```bash
docker build -t kafka-schema-reg-migrator .
```

### 2. Run Tests in Container

```bash
docker run -it --rm \
  --network host \
  -v $(pwd):/app \
  kafka-schema-reg-migrator \
  bash -c "cd tests && ./run_tests.sh"
```

## Test Cases

The test suite includes the following test cases:

1. **Comparison-only Test**
   - Compares source and destination registries
   - No changes are made to either registry
   - Verifies schema statistics and version information

2. **Cleanup Test**
   - Runs with `CLEANUP_DESTINATION=true` and `DRY_RUN=true`
   - Verifies that the destination registry is empty after cleanup
   - No actual migration is performed

3. **Normal Migration Test**
   - Migrates schemas from source to destination
   - Verifies successful migration
   - Checks schema versions and compatibility

4. **Import Mode Migration Test**
   - Migrates schemas in import mode
   - Verifies that schema IDs are preserved
   - Checks schema compatibility

5. **Context Migration Test**
   - Migrates schemas from default context to a specific context
   - Verifies context creation and schema migration
   - Checks subject naming in the destination context

## Test Schemas

The test environment uses the following schema evolution:

1. Basic schema with id and name
2. Schema with added optional description field
3. Schema with added array of tags

This provides coverage for:
- Basic schema registration
- Schema evolution
- Optional fields
- Array types
- Default values
- Context-aware schema migration

## Troubleshooting

### Common Issues

1. **Docker Compose Not Found**
   - Ensure Docker Compose is installed
   - Try using `docker compose` instead of `docker-compose`

2. **Port Conflicts**
   - Check if ports 8081 and 8082 are available
   - Stop any running Schema Registry instances

3. **Test Failures**
   - Check Docker logs: `docker logs <container_id>`
   - Verify network connectivity between containers
   - Ensure sufficient wait time for services to start

### Debugging

1. **View Container Logs**
```bash
docker logs <container_id>
```

2. **Check Container Status**
```bash
docker ps
docker-compose ps
```

3. **Inspect Network**
```bash
docker network inspect tests_default
```

4. **Manual Testing**
```bash
# Start the test environment
cd tests
docker-compose up -d

# Run a specific test
python test_migration.py

# Clean up
docker-compose down
```

## Adding New Tests

1. Create a new test file in the `tests` directory
2. Follow the existing test patterns
3. Add test cases to `run_tests.sh`
4. Update this documentation if necessary 