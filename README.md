# Kafka Schema Registry Migrator

A tool for migrating schemas between Kafka Schema Registry instances, with support for contexts, import mode, and ID collision handling.

## Features

- **Schema Comparison**: Compare schemas between source and destination registries
- **ID Collision Detection**: Detect and report schema ID conflicts before migration
- **Dry Run Mode**: Test migration without making actual changes
- **Import Mode Support**: Preserve schema IDs during migration (requires compatible Schema Registry)
- **Context Support**: Migrate schemas between different contexts or clusters
- **Authentication**: Support for basic authentication on both registries
- **Cleanup Option**: Optionally clean destination registry before migration
- **Detailed Logging**: Comprehensive logging of all operations
- **Docker Support**: Easy deployment using Docker
- **Kubernetes Support**: Deploy as a Kubernetes Job
- **Read-Only Subject Handling**: Automatically handles subjects in read-only mode during migration
- **Schema Type Support**: Supports migration of AVRO, JSON, and PROTOBUF schemas

## QuickStart

### Comparison Only

To compare schemas between two registries without making any changes:

```bash
# Using Docker
docker run -it --env-file .env kafka-schema-reg-migrator

# Using Python
python schema_registry_migrator.py
```

Required environment variables:
```bash
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081
ENABLE_MIGRATION=false
```

This will:
- Compare schemas between source and destination
- Show schema statistics and versions
- Identify potential ID collisions
- Display version differences
- No changes are made to either registry

### Migration

To migrate schemas from source to destination:

```bash
# Using Docker
docker run -it --env-file .env kafka-schema-reg-migrator

# Using Python
python schema_registry_migrator.py
```

Required environment variables:
```bash
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081
ENABLE_MIGRATION=true
DRY_RUN=false
```

Optional settings:
```bash
# To preserve schema IDs
DEST_IMPORT_MODE=true

# To clean up destination before migration
CLEANUP_DESTINATION=true

# To use specific contexts
SOURCE_CONTEXT=source-context
DEST_CONTEXT=dest-context
```

This will:
- Compare schemas between registries
- Check for ID collisions
- Migrate schemas from source to destination
- Preserve schema IDs if import mode is enabled
- Clean up destination if specified
- Show migration results and statistics
- Validate the migration by checking for any missing subjects or versions
- Provide warnings and suggestions if any discrepancies are found

## Prerequisites

- **Python 3.8+** (for running scripts and tests locally)
- **Docker** (for running the test environment and/or containerized usage)
- **Docker Compose** (for orchestrating multi-container test environments; v2 recommended)
- **git** (optional, for cloning the repository)

## Installation

### Option 1: Local Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Option 2: Docker Installation

1. Clone this repository
2. Build the Docker image:
```bash
docker build -t kafka-schema-reg-migrator .
```

### Using Docker

```bash
docker pull aywengo/kafka-schema-reg-migrator:latest
```

### From Source

1. Clone the repository:
```bash
git clone https://github.com/aywengo/kafka-schema-reg-migrator.git
cd kafka-schema-reg-migrator
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with the following variables:

### Required Variables
```
SOURCE_SCHEMA_REGISTRY_URL=http://source-schema-registry:8081
DEST_SCHEMA_REGISTRY_URL=http://dest-schema-registry:8081
```

### Optional Authentication
```
SOURCE_USERNAME=optional_username
SOURCE_PASSWORD=optional_password
DEST_USERNAME=optional_username
DEST_PASSWORD=optional_password
```

### Optional Context
```
SOURCE_CONTEXT=optional_context
DEST_CONTEXT=optional_context
```

### Migration Control
```
ENABLE_MIGRATION=false    # Set to true to enable migration
DRY_RUN=true             # Set to false to perform actual migration
DEST_IMPORT_MODE=false   # Set to true to enable import mode for destination
CLEANUP_DESTINATION=false # Set to true to clean up destination before migration
PRESERVE_IDS=false       # Set to true to preserve original schema IDs
RETRY_FAILED=true        # Set to false to disable retry of failed migrations
PERMANENT_DELETE=true    # Set to false to use soft delete when cleaning up
DEST_MODE_AFTER_MIGRATION=READWRITE  # Global mode to set after migration (READWRITE, READONLY, READWRITE_OVERRIDE)
```

## Usage

### Local Usage

Run the script:
```bash
python schema_registry_migrator.py
```

### Docker Usage

Run the container:
```bash
docker run --env-file .env kafka-schema-reg-migrator
```

For interactive mode (to run tests or other commands):
```bash
docker run -it --env-file .env kafka-schema-reg-migrator /bin/bash
```

To run tests in Docker:
```bash
docker run -it --env-file .env kafka-schema-reg-migrator bash -c "cd tests && ./run_tests.sh"
```

### Environment Variables

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

### ID Collision Handling

The tool detects and handles ID collisions between source and destination registries:

1. If `CLEANUP_DESTINATION=false` (default):
   - ID collisions will stop the migration
   - A detailed report of collisions will be provided
   - Options for resolution will be suggested

2. If `CLEANUP_DESTINATION=true`:
   - ID collisions will be logged as informational messages
   - The destination registry will be cleaned up before migration
   - Migration will proceed normally

### ID Preservation with Subject-Level IMPORT Mode

When `PRESERVE_IDS=true`, the tool uses subject-level IMPORT mode to preserve schema IDs:

1. **Subject-level IMPORT mode**: Before registering each schema, the tool sets the specific subject to IMPORT mode
2. **Register with ID**: The schema is registered with its original ID from the source registry
3. **Restore mode**: After registration, the subject is returned to its original mode

This approach follows the official Confluent Schema Registry migration process:
- The subject must be empty or non-existent to set IMPORT mode
- If a subject already has schemas, ID preservation will be skipped for that subject
- This is independent of the global IMPORT mode (`DEST_IMPORT_MODE`)

### Global vs Subject-Level Modes

- **`DEST_IMPORT_MODE`**: Sets the global mode of the destination registry to IMPORT
- **`DEST_MODE_AFTER_MIGRATION`**: Sets the global mode after migration completes
- **`PRESERVE_IDS`**: Uses subject-level IMPORT mode for each subject during migration

### Read-Only Subject Handling

The tool automatically handles subjects that are in read-only mode:
- Detects subjects not in READWRITE mode
- Temporarily changes them to READWRITE for migration
- Restores the original mode after migration
- Failed migrations are automatically retried with mode changes (if `RETRY_FAILED=true`)

### Conflict Handling

The tool handles 409 Conflict errors gracefully:
- When a schema already exists in the destination, it will be skipped
- The tool verifies if the existing schema matches the source schema
- Conflicts are logged and reported in the migration summary
- This prevents duplicate schema registrations and ensures idempotent migrations

### Selective Subject Cleanup

You can clean up specific subjects before migration without affecting others:

```bash
# Clean up only specific subjects
CLEANUP_SUBJECTS=subject1,subject2,subject3
PERMANENT_DELETE=true
```

This is useful when:
- You have version mismatches in specific subjects
- You want to re-migrate only certain subjects
- You need to resolve conflicts without affecting the entire registry

### Enhanced Error Reporting

When schema conflicts occur (409 errors), the tool now provides detailed information about the differences:
- Field-level differences for AVRO schemas
- Namespace and type differences
- Clear indication of what fields exist only in source or destination

Example error output:
```
ERROR - 409 Conflict for user-events: schema content differs from existing versions. Latest version in destination: 20
ERROR - Schema differences for user-events version 9:
ERROR -   - Fields only in source: newField1, newField2
ERROR -   - Fields only in destination: oldField1
ERROR -   - Namespace differs: source='com.example.v2', dest='com.example.v1'
```

### Running Tests

The test suite includes both integration tests and unit tests:

```bash
# Run all tests with pytest
pytest tests/test_migration.py -v

# Run integration tests only
python tests/test_migration.py
```

The test environment includes:
- Source Schema Registry (port 38081)
- Destination Schema Registry (port 38082)
- AKHQ UI (port 38080) - A modern UI for managing both Kafka clusters and Schema Registries

## Docker Support

See [docs/run-in-docker.md](docs/run-in-docker.md) for Docker usage instructions.

## Documentation

- [Migration Flow Diagrams](migration-flow.md)
- [Running Tests](docs/running-tests.md)
- [Docker Usage](docs/run-in-docker.md)
- [Environment Variables](docs/run-in-docker.md#available-environment-variables)

## License

See [LICENSE](LICENSE) file for details.