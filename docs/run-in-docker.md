# Run in Docker Container

This document describes how to pass environment variables to the Kafka Schema Registry Migrator Docker container.

## Basic Usage

You can pass environment variables to the container using the `-e` or `--env` flag:

```bash
docker run -e SOURCE_SCHEMA_REGISTRY_URL=http://source:8081 \
           -e DEST_SCHEMA_REGISTRY_URL=http://dest:8081 \
           -e ENABLE_MIGRATION=true \
           kafka-schema-reg-migrator:latest
```

## Available Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `SOURCE_SCHEMA_REGISTRY_URL` | URL of the source Schema Registry | Yes | - |
| `DEST_SCHEMA_REGISTRY_URL` | URL of the destination Schema Registry | Yes | - |
| `ENABLE_MIGRATION` | Enable schema migration | Yes | - |
| `DRY_RUN` | Run in dry-run mode (no actual changes) | No | true |
| `SOURCE_USERNAME` | Username for source Schema Registry authentication | No | - |
| `SOURCE_PASSWORD` | Password for source Schema Registry authentication | No | - |
| `DEST_USERNAME` | Username for destination Schema Registry authentication | No | - |
| `DEST_PASSWORD` | Password for destination Schema Registry authentication | No | - |
| `SOURCE_CONTEXT` | Context name for source Schema Registry | No | - |
| `DEST_CONTEXT` | Context name for destination Schema Registry | No | - |
| `DEST_IMPORT_MODE` | Set global IMPORT mode on destination registry | No | false |
| `CLEANUP_DESTINATION` | Delete all subjects in destination registry before migration | No | false |
| `CLEANUP_SUBJECTS` | Comma-separated list of specific subjects to delete before migration | No | - |
| `PRESERVE_IDS` | Preserve original schema IDs during migration (uses subject-level IMPORT mode) | No | false |
| `RETRY_FAILED` | Automatically retry failed migrations | No | true |
| `PERMANENT_DELETE` | Use permanent (hard) delete when cleaning up destination | No | true |
| `DEST_MODE_AFTER_MIGRATION` | Global mode to set after migration (READWRITE, READONLY, READWRITE_OVERRIDE) | No | READWRITE |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | No | INFO |

## Using Environment File

For multiple environment variables, you can use an environment file:

1. Create a `.env` file:
```bash
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081
ENABLE_MIGRATION=true
DRY_RUN=true
SOURCE_USERNAME=source_user
SOURCE_PASSWORD=source_pass
DEST_USERNAME=dest_user
DEST_PASSWORD=dest_pass
SOURCE_CONTEXT=source-context
DEST_CONTEXT=dest-context
DEST_IMPORT_MODE=false
CLEANUP_DESTINATION=false
CLEANUP_SUBJECTS=
PRESERVE_IDS=false
RETRY_FAILED=true
PERMANENT_DELETE=true
DEST_MODE_AFTER_MIGRATION=READWRITE
LOG_LEVEL=DEBUG
```

2. Run the container with the environment file:
```bash
docker run --env-file .env kafka-schema-reg-migrator:latest
```

## Import Mode

Schema Registry supports two types of import modes:

### Global Import Mode (`DEST_IMPORT_MODE`)

The `DEST_IMPORT_MODE` setting enables global IMPORT mode on the destination registry. This affects all operations on the registry.

### Subject-Level Import Mode (`PRESERVE_IDS`)

When `PRESERVE_IDS` is set to `true`, the tool uses subject-level IMPORT mode for ID preservation:

1. Before registering each schema, the specific subject is set to IMPORT mode
2. The schema is registered with its original ID from the source registry  
3. After registration, the subject is returned to its original mode

This follows the official Confluent Schema Registry migration process:
- The subject must be empty or non-existent to set IMPORT mode
- If a subject already has schemas, ID preservation will be skipped for that subject
- Subject-level IMPORT mode is used regardless of the global mode setting

Example configuration for ID preservation:
```bash
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081
ENABLE_MIGRATION=true
PRESERVE_IDS=true  # Uses subject-level IMPORT mode for each subject
CLEANUP_DESTINATION=true  # Recommended to ensure subjects are empty
```

## Example with Docker Compose

```yaml
version: '3'
services:
  migrator:
    image: kafka-schema-reg-migrator:latest
    environment:
      - SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
      - DEST_SCHEMA_REGISTRY_URL=http://dest:8081
      - ENABLE_MIGRATION=true
      - DRY_RUN=true
      - SOURCE_USERNAME=source_user
      - SOURCE_PASSWORD=source_pass
      - DEST_USERNAME=dest_user
      - DEST_PASSWORD=dest_pass
      - SOURCE_CONTEXT=source-context
      - DEST_CONTEXT=dest-context
      - DEST_IMPORT_MODE=false
      - CLEANUP_DESTINATION=false
      - CLEANUP_SUBJECTS=
      - PRESERVE_IDS=false
      - RETRY_FAILED=true
      - PERMANENT_DELETE=true
      - DEST_MODE_AFTER_MIGRATION=READWRITE
      - LOG_LEVEL=DEBUG
```

## Security Considerations

1. Never commit `.env` files to version control
2. Use secrets management for sensitive values
3. Consider using Docker secrets in production environments

## Troubleshooting

If you encounter issues with environment variables:

1. Verify the variables are set correctly:
```bash
docker run --rm kafka-schema-reg-migrator:latest env
```

2. Check the container logs:
```bash
docker logs <container_id>
```

3. Ensure the environment variables are properly formatted and contain valid values

## ID Collision Handling

The tool detects and handles ID collisions between source and destination registries:

1. If `CLEANUP_DESTINATION=false` (default):
   - ID collisions will stop the migration
   - A detailed report of collisions will be provided
   - Options for resolution will be suggested

2. If `CLEANUP_DESTINATION=true`:
   - ID collisions will be logged as informational messages
   - The destination registry will be cleaned up before migration
   - Migration will proceed normally

## Read-Only Subject Handling

The migrator automatically handles subjects in read-only mode:
- Temporarily changes read-only subjects to READWRITE mode
- Performs the migration
- Restores the original mode
- Failed migrations are retried with mode changes (if `RETRY_FAILED=true`)

## Running Tests in Docker

The test suite includes both integration tests and unit tests:

```bash
# Run all tests with pytest
docker run -it kafka-schema-reg-migrator:latest pytest tests/test_migration.py -v

# Run integration tests only
docker run -it kafka-schema-reg-migrator:latest python tests/test_migration.py
```

## Test Environment

The test environment consists of:
- Two independent Kafka clusters (source and destination)
- Two Schema Registry instances
- AKHQ for monitoring and management
- Test schemas with different versions
- Automated test scripts

For detailed information about running tests, see [Running Tests](running-tests.md) 