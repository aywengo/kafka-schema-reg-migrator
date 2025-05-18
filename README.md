# Kafka Schema Registry Migrator

This tool helps compare and analyze schemas between two Kafka Schema Registries. It can be used to identify potential ID collisions and schema version differences between source and destination registries, and optionally migrate schemas from source to destination.

## Features

- Connect to source Schema Registry in read-only mode
- Optional basic authentication support
- Display schema statistics and versions
- Connect to destination Schema Registry
- Compare schemas between registries
- Identify potential ID collisions
- Support for schema registry context
- Optional schema migration with dry run support
- Import mode support for destination registry
- Docker support for easy deployment

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

## Output

The tool will display:
- List of all schemas with their versions from both registries
- Comparison of schema IDs between source and destination
- Potential ID collisions
- Schema version differences

If migration is enabled (`ENABLE_MIGRATION=true`), it will also show:
- Migration results (successful, failed, skipped)
- New schema IDs assigned in destination
- Compatibility check results (in dry run mode)

## Migration Modes

### Comparison Only (Default)
- Set `ENABLE_MIGRATION=false` (default)
- Only compares schemas between registries
- Shows statistics and potential issues
- No changes are made to either registry

### Dry Run Migration
- Set `ENABLE_MIGRATION=true`
- Set `DRY_RUN=true` (default)
- Performs compatibility checks
- Shows what would be migrated
- No actual changes are made

### Actual Migration
- Set `ENABLE_MIGRATION=true`
- Set `DRY_RUN=false`
- Performs actual schema migration
- Preserves schema IDs if import mode is enabled
- Optionally cleans up destination before migration if `CLEANUP_DESTINATION=true`

### Import Mode
- Set `DEST_IMPORT_MODE=true` to enable
- Preserves original schema IDs during migration
- Useful when migrating between registries that should maintain the same IDs
- Requires appropriate permissions on destination registry

### Cleanup Mode
- Set `CLEANUP_DESTINATION=true` to enable
- Deletes all subjects from destination registry before migration
- Useful when you want to start with a clean destination
- Only works when `ENABLE_MIGRATION=true`
- **If you want to only clean up (without migrating), set `DRY_RUN=true` as well.**
- Use with caution as it will delete all existing schemas in the destination

## Safety Features

- Dry run mode by default
- Schema compatibility checking
- Version ordering to ensure proper migration sequence
- Skip existing schemas to avoid duplicates
- Detailed logging of all operations
- Optional import mode to preserve schema IDs
- Optional cleanup mode for fresh migration

## Testing

The project includes a complete test environment that runs two independent Schema Registries using Docker. The test suite verifies both normal migration and import mode functionality.

### Test Environment

The test environment consists of:
- Two independent Kafka clusters (source and destination)
- Two Schema Registry instances
- Test schemas with different versions
- Automated test scripts

### Running Tests

#### Local Testing

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

#### Docker Testing

Run the tests in Docker:
```bash
docker run -it --env-file .env kafka-schema-migrator bash -c "cd tests && ./run_tests.sh"
```

The test suite will:
1. Start two schema registries in Docker
2. Populate the source registry with test schemas
3. Run the following tests in sequence:
   - **Comparison-only test**: Compares source and destination registries, no changes made.
   - **Cleanup test**: Runs the migration script with `CLEANUP_DESTINATION=true` and `DRY_RUN=true` to ensure the destination registry is empty after cleanup (no migration performed).
   - **Normal migration test**: Migrates schemas from source to destination and verifies the result.
   - **Import mode migration test**: Migrates schemas in import mode and verifies ID preservation.
4. Clean up the environment

### Test Cases

The test suite includes:
- Comparison-only mode test
- Cleanup-only test (no migration, just cleanup)
- Normal migration test
- Import mode migration test

### Test Schemas

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