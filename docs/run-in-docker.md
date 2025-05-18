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
LOG_LEVEL=DEBUG
```

2. Run the container with the environment file:
```bash
docker run --env-file .env kafka-schema-reg-migrator:latest
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