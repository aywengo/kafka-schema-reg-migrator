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

### Prerequisites

1. Docker and Docker Compose installed
2. Python 3.8+ installed
3. Required Python packages installed:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Test Suite

1. Navigate to the tests directory:
   ```bash
   cd tests
   ```

2. Run the test script:
   ```bash
   ./run_tests.sh
   ```

   For debugging:
   ```bash
   ./run_tests.sh --debug
   ```

### Test Components

The test environment includes:

1. **Kafka Clusters**:
   - Source cluster (port 39092)
   - Destination cluster (port 39093)

2. **Schema Registries**:
   - Source registry (port 38081)
   - Destination registry (port 38082)

3. **AKHQ UI**:
   - Single UI for both clusters (port 38080)

### Accessing the UI

You can access the AKHQ UI at:
- http://localhost:38080

The AKHQ UI provides a comprehensive interface to:
- Browse and search schemas
- View schema versions
- Compare schema versions
- Manage subjects and versions
- Monitor Kafka clusters
- View consumer groups
- Browse topics and messages

### Test Cases

The test suite includes:

1. Comparison-only test
2. Cleanup test
3. Normal migration test
4. Import mode migration test
5. Context migration test
6. Same cluster context migration test
7. Authentication validation test
8. ID collision with cleanup test
9. ID collision without cleanup test
10. ID collision with cleanup and import mode test

### Debugging

If you need to debug the test environment:

1. Run tests in debug mode:
   ```bash
   ./run_tests.sh --debug
   ```

2. This will:
   - Keep the environment running after tests
   - Show detailed logs
   - Allow manual inspection of the environment

3. To clean up manually:
   ```bash
   docker-compose down
   ```

### Troubleshooting

If you encounter issues:

1. Check if all services are running:
   ```bash
   docker-compose ps
   ```

2. View service logs:
   ```bash
   docker-compose logs
   ```

3. Check AKHQ UI:
   - http://localhost:38080

4. Verify ports are not in use:
   ```bash
   lsof -i :38081,38082,38080,39092,39093
   ```

## Test Environment

The test environment consists of:
- Source Kafka cluster (port 39092)
- Destination Kafka cluster (port 39093)
- Source Schema Registry (port 38081)
- Destination Schema Registry (port 38082)
- AKHQ UI (port 38080)

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

## Cleanup

After running tests, you can clean up the test environment:

```bash
docker-compose down -v
```

This will:
- Stop all containers
- Remove volumes
- Clean up networks 