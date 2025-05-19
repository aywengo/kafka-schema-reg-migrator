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
| `DEST_IMPORT_MODE` | Enable import mode to preserve schema IDs during migration | No | false |
| `CLEANUP_DESTINATION` | Delete all subjects in destination registry before migration | No | false |
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
LOG_LEVEL=DEBUG
```

2. Run the container with the environment file:
```bash
docker run --env-file .env kafka-schema-reg-migrator:latest
```

## Import Mode

The `DEST_IMPORT_MODE` setting enables a special mode for schema migration that preserves the original schema IDs from the source registry. This is particularly useful when:

- You need to maintain the same schema IDs across registries
- Downstream systems depend on specific schema IDs
- You need to ensure exact schema ID matching between source and destination

When `DEST_IMPORT_MODE` is set to `true`:
- The tool adds a special header `X-Registry-Import: true` to schema registration requests
- The Schema Registry preserves the original schema IDs instead of generating new ones
- Schema content and version history remain unchanged
- Only the schema IDs are preserved

Note that using import mode requires appropriate permissions on the destination registry. The import mode works in conjunction with other settings like `ENABLE_MIGRATION` and `DRY_RUN`.

Example configuration for import mode:
```bash
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081
ENABLE_MIGRATION=true
DEST_IMPORT_MODE=true  # Enable import mode to preserve schema IDs
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