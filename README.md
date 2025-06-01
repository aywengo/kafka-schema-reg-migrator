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

### Option 3: Using DockerHub

```bash
docker pull aywengo/kafka-schema-reg-migrator:latest
```

## Configuration

Create a `.env` file with the following variables:

### Required Variables
```
SOURCE_SCHEMA_REGISTRY_URL=http://source-schema-registry:8081
DEST_SCHEMA_REGISTRY_URL=http://dest-schema-registry:8081
ENABLE_MIGRATION=false    # Set to true to enable migration
```

### Optional Variables
```
# Authentication
SOURCE_USERNAME=optional_username
SOURCE_PASSWORD=optional_password
DEST_USERNAME=optional_username
DEST_PASSWORD=optional_password

# Context
SOURCE_CONTEXT=optional_context
DEST_CONTEXT=optional_context

# Migration Control
DRY_RUN=true             # Set to false to perform actual migration
DEST_IMPORT_MODE=false   # Set to true to enable import mode for destination (requires Schema Registry 7.0+)
CLEANUP_DESTINATION=false # Set to true to clean up destination before migration
PRESERVE_IDS=false       # Set to true to preserve original schema IDs
RETRY_FAILED=true        # Set to false to disable retry of failed migrations
PERMANENT_DELETE=true    # Set to false to use soft delete when cleaning up
DEST_MODE_AFTER_MIGRATION=READWRITE  # Global mode to set after migration
LOG_LEVEL=INFO          # Logging level (DEBUG, INFO, WARNING, ERROR)
```

## Usage

### Comparison Mode

To compare schemas between two registries without making any changes:

```bash
# Using Docker
docker run -it --env-file .env kafka-schema-reg-migrator

# Using Python
python schema_registry_migrator.py
```

Required `.env` file for comparison:
```bash
# Required URLs for source and destination registries
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081

# Disable migration to run in comparison mode only
ENABLE_MIGRATION=false

# Optional: Add authentication if required
SOURCE_USERNAME=optional_username
SOURCE_PASSWORD=optional_password
DEST_USERNAME=optional_username
DEST_PASSWORD=optional_password
```

This will:
- Compare schemas between source and destination
- Show schema statistics and versions
- Identify potential ID collisions
- Display version differences
- No changes are made to either registry

### Migration Mode

To migrate schemas from source to destination:

```bash
# Using Docker
docker run -it --env-file .env kafka-schema-reg-migrator

# Using Python
python schema_registry_migrator.py
```

Required `.env` file for migration:
```bash
# Required URLs for source and destination registries
SOURCE_SCHEMA_REGISTRY_URL=http://source:8081
DEST_SCHEMA_REGISTRY_URL=http://dest:8081

# Enable migration mode
ENABLE_MIGRATION=true
DRY_RUN=false

# Optional: Preserve schema IDs during migration
DEST_IMPORT_MODE=true

# Optional: Clean up destination before migration
CLEANUP_DESTINATION=true

# Optional: Use specific contexts if needed
SOURCE_CONTEXT=source-context
DEST_CONTEXT=dest-context

# Optional: Authentication if required
SOURCE_USERNAME=optional_username
SOURCE_PASSWORD=optional_password
DEST_USERNAME=optional_username
DEST_PASSWORD=optional_password
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

## Advanced Features

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

### ID Preservation

When `PRESERVE_IDS=true`, the tool uses subject-level IMPORT mode to preserve schema IDs. Note that this feature requires Schema Registry version 7.0 or above.

1. **Subject-level IMPORT mode**: Before registering each schema, the tool sets the specific subject to IMPORT mode
2. **Register with ID**: The schema is registered with its original ID from the source registry
3. **Restore mode**: After registration, the subject is returned to its original mode

### Read-Only Subject Handling

The tool automatically handles subjects that are in read-only mode:
- Detects subjects not in READWRITE mode
- Temporarily changes them to READWRITE for migration
- Restores the original mode after migration
- Failed migrations are automatically retried with mode changes (if `RETRY_FAILED=true`)

### Selective Subject Cleanup

You can clean up specific subjects before migration without affecting others:

```bash
# Clean up only specific subjects
CLEANUP_SUBJECTS=subject1,subject2,subject3
PERMANENT_DELETE=true
```

## Testing

The test suite includes both integration tests and unit tests:

```bash
cd tests
./run_tests.sh

# Run tests with debug mode
./run_tests.sh --debug

# Run test environment without testing
docker compose up -d
```

The test environment includes:
- Source Schema Registry (port 38081)
- Destination Schema Registry (port 38082)
- AKHQ UI (port 38080) - A modern UI for managing both Kafka clusters and Schema Registries

## Documentation

- [Migration Flow Diagrams](migration-flow.md)
- [Running Tests](docs/running-tests.md)
- [Docker Usage](docs/run-in-docker.md)
- [Environment Variables](docs/run-in-docker.md#available-environment-variables)

## License

See [LICENSE](LICENSE) file for details.
