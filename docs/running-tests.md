# Running Tests

This document describes how to run the test suite for the Kafka Schema Registry Migrator.

## Test Structure

The test suite includes both integration tests and unit tests:

1. Integration Tests:
   - Comparison-only test
   - Cleanup test
   - Normal migration test
   - Import mode migration test
   - Context migration test
   - Same cluster context migration test

2. Unit Tests:
   - Authentication validation
   - ID collision handling with cleanup
   - ID collision handling without cleanup
   - ID collision handling with import mode

## Running Tests

### Using pytest

To run all tests with pytest:

```bash
pytest tests/test_migration.py -v
```

### Running Integration Tests Only

To run only the integration tests:

```bash
python tests/test_migration.py
```

## Test Environment

The test environment consists of:
- Two independent Kafka clusters (source and destination)
- Two Schema Registry instances
- AKHQ for monitoring and management
- Test schemas with different versions
- Automated test scripts

### Starting the Test Environment

1. Make sure you have Docker and Docker Compose installed
2. Make the test script executable:
```bash
chmod +x tests/run_tests.sh
```

3. Run the tests:
```bash
cd tests
./run_tests.sh
```

## Test Cases

### Integration Tests

1. **Comparison-only test**:
   - Compares source and destination registries
   - No changes are made
   - Verifies schema comparison functionality

2. **Cleanup test**:
   - Runs with `CLEANUP_DESTINATION=true` and `DRY_RUN=true`
   - Verifies destination registry cleanup
   - No migration is performed

3. **Normal migration test**:
   - Migrates schemas from source to destination
   - Verifies schema content and versions
   - Checks for proper migration order

4. **Import mode migration test**:
   - Migrates schemas with `DEST_IMPORT_MODE=true`
   - Verifies schema ID preservation
   - Checks for proper schema content

5. **Context migration test**:
   - Migrates schemas between different contexts
   - Verifies context-aware schema handling
   - Checks for proper context prefixes

6. **Same cluster context migration test**:
   - Migrates schemas between contexts in the same cluster
   - Verifies proper context isolation
   - Checks for schema content preservation

### Unit Tests

1. **Authentication validation**:
   - Tests username/password validation
   - Verifies proper error handling
   - Checks authentication requirements

2. **ID collision handling**:
   - Tests with `CLEANUP_DESTINATION=true`:
     - Verifies cleanup before migration
     - Checks for proper collision logging
     - Ensures migration proceeds
   - Tests with `CLEANUP_DESTINATION=false`:
     - Verifies migration stops on collision
     - Checks for proper error reporting
     - Ensures no changes are made
   - Tests with import mode:
     - Verifies proper header handling
     - Checks for ID preservation
     - Ensures schema content integrity

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

1. **Schema Registry not ready**:
   - Check if the services are running: `docker-compose ps`
   - Check service logs: `docker-compose logs schema-registry-source schema-registry-dest`
   - Wait for services to be fully ready (usually 30-60 seconds)

2. **Test failures**:
   - Check the test logs for detailed error messages
   - Verify environment variables are set correctly
   - Ensure no other services are using the required ports

3. **ID collision errors**:
   - Check if `CLEANUP_DESTINATION` is set appropriately
   - Verify schema IDs in both registries
   - Consider using a different context if needed

### Debug Mode

To run tests in debug mode:

```bash
DEBUG=true ./run_tests.sh
```

This will:
- Show more detailed logs
- Display API responses
- Provide additional debugging information

## Cleanup

After running tests, you can clean up the test environment:

```bash
docker-compose down -v
```

This will:
- Stop all containers
- Remove volumes
- Clean up networks 