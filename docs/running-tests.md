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
- AKHQ (Kafka UI) for managing and viewing Kafka clusters and Schema Registries
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

You can also run tests in debug mode, which keeps the environment running even if tests fail:

```bash
cd tests
./run_tests.sh --debug
```

Debug mode is useful when you want to:
- Inspect the environment after a test failure
- Run tests manually
- Debug schema registry issues
- Check UI state after test failures

When running in debug mode:
- The environment (Docker containers) stays up even if tests fail
- You can manually inspect the Schema Registry UI
- You can run individual tests manually
- You need to clean up manually using `docker-compose down`

### 3. Access Kafka and Schema Registry UI

The test environment includes AKHQ, a modern web-based UI that provides access to both Kafka clusters and Schema Registries:

- AKHQ UI: http://localhost:38090

The UI provides the following features:
- View and manage Kafka topics
- View and manage Schema Registry subjects
- Monitor consumer groups
- View schema versions and compatibility
- Create and delete topics
- View topic configurations
- Monitor cluster health
- Switch between source and destination clusters

To use the UI:
1. Open http://localhost:38090 in your browser
2. Use the cluster selector in the top-right to switch between source and destination
3. Access Schema Registry features through the left menu
4. View topics, consumer groups, and other Kafka features

## Test Cases

The test suite includes the following test cases:

1. **Authentication Validation Test**
   - Tests username/password validation logic
   - Verifies that both credentials must be provided together or neither
   - Tests four scenarios:
     * Both username and password provided (should succeed)
     * Neither username nor password provided (should succeed)
     * Only username provided (should fail)
     * Only password provided (should fail)
   - Ensures proper error messages for invalid combinations

2. **Comparison-only Test**
   - Compares source and destination registries
   - No changes are made to either registry
   - Verifies schema statistics and version information

3. **Cleanup Test**
   - Runs with `CLEANUP_DESTINATION=true` and `DRY_RUN=true`
   - Verifies that the destination registry is empty after cleanup
   - No actual migration is performed

4. **Normal Migration Test**
   - Migrates schemas from source to destination
   - Verifies successful migration
   - Checks schema versions and compatibility

5. **Import Mode Migration Test**
   - Migrates schemas in import mode
   - Verifies that schema IDs are preserved
   - Checks schema compatibility

6. **Context Migration Test**
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
   - Check if ports 38081, 38082, and 38090 are available
   - Stop any running Schema Registry instances or UI services

3. **Test Failures**
   - Check Docker logs: `docker logs <container_id>`
   - Verify network connectivity between containers
   - Ensure sufficient wait time for services to start

4. **UI Access Issues**
   - Verify that both Schema Registry instances are running
   - Check UI container logs for connection issues
   - Ensure no firewall rules are blocking the ports

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

5. **UI Troubleshooting**
```bash
# Check UI container logs
docker logs tests_akhq_1

# Verify Schema Registry connectivity
curl http://localhost:38081/subjects
curl http://localhost:38082/subjects

# Verify Kafka connectivity
kafka-topics --bootstrap-server localhost:39092 --list
kafka-topics --bootstrap-server localhost:39093 --list
```

## Adding New Tests

1. Create a new test file in the `tests` directory
2. Follow the existing test patterns
3. Add test cases to `run_tests.sh`
4. Update this documentation if necessary 