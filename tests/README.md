# Schema Registry Migrator Tests

This directory contains comprehensive tests for the Schema Registry Migrator tool.

## Test Environment

The test environment uses Docker Compose to set up:
- Two Kafka clusters (source and destination)
- Two Schema Registry instances (source and destination)
- AKHQ UI for visual inspection

## Running Tests

### Quick Start

```bash
cd tests
./run_tests.sh
```

### Debug Mode

To keep the test environment running after tests complete (useful for debugging):

```bash
./run_tests.sh --debug
```

## Test Cases

### 1. Comparison-only Test
Tests the comparison functionality without performing any migration.

### 2. Cleanup Test
Tests the destination registry cleanup functionality.

### 3. Normal Migration Test
Tests basic schema migration from source to destination.

### 4. Import Mode Migration Test
Tests migration with import mode enabled (preserves schema IDs).

### 5. Context Migration Test
Tests migration between different contexts.

### 6. Same Cluster Context Migration Test
Tests migration between contexts within the same cluster.

### 7. Authentication Validation Test
Tests authentication parameter validation.

### 8. ID Collision with Cleanup Test
Tests handling of ID collisions when cleanup is enabled.

### 9. ID Collision without Cleanup Test
Tests handling of ID collisions when cleanup is disabled (should fail).

### 10. ID Collision with Cleanup and Import Mode Test
Tests ID collision handling with both cleanup and import mode enabled.

### 11. Subject Mode API Test
Tests the subject mode API methods (get/set mode).

### 12. Migration with Read-Only Subjects Test
Tests automatic handling of subjects in read-only mode:
- Sets up a subject in READONLY mode
- Attempts migration
- Verifies the tool temporarily changes mode to READWRITE
- Confirms mode is restored after migration

### 13. Migration with ID Preservation Test
Tests schema migration with ID preservation enabled:
- Enables PRESERVE_IDS flag
- Migrates schemas
- Verifies original IDs are preserved

### 14. Retry Failed Migrations Test
Tests the automatic retry mechanism:
- Creates a scenario where migration fails initially
- Verifies retry is triggered
- Confirms successful migration after retry
- Verifies subject modes are properly restored

## Test Infrastructure

### Docker Services

- **kafka-source**: Source Kafka cluster (port 39092)
- **kafka-dest**: Destination Kafka cluster (port 39093)
- **schema-registry-source**: Source Schema Registry (port 38081)
- **schema-registry-dest**: Destination Schema Registry (port 38082)
- **akhq-ui**: Web UI for Kafka and Schema Registry (port 38080)

### Test Scripts

- `run_tests.sh`: Main test runner script
- `test_migration.py`: Python test suite
- `populate_source.py`: Populates source registry with test schemas

## Accessing the Test Environment

While tests are running or in debug mode:

- Source Schema Registry: http://localhost:38081
- Destination Schema Registry: http://localhost:38082
- AKHQ UI: http://localhost:38080

## Troubleshooting

### Tests Failing

1. Check Docker logs:
   ```bash
   docker-compose logs schema-registry-source
   docker-compose logs schema-registry-dest
   ```

2. Verify services are healthy:
   ```bash
   docker-compose ps
   ```

3. Clean up and restart:
   ```bash
   docker-compose down -v
   ./run_tests.sh
   ```

### Port Conflicts

If you get port binding errors, check for conflicting services:
```bash
lsof -i :38081
lsof -i :38082
lsof -i :38080
``` 