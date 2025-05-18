#!/usr/bin/env python3

import os
import json
import logging
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SchemaRegistryClient:
    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        context: Optional[str] = None,
        import_mode: bool = False
    ):
        self.url = url.rstrip('/')
        self.auth = (username, password) if username and password else None
        self.context = context
        self.import_mode = import_mode
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        logger.info(f"Initialized SchemaRegistryClient for {url} (import_mode: {import_mode})")

    def _get_url(self, path: str) -> str:
        """Construct URL with optional context."""
        if self.context:
            return f"{self.url}/contexts/{self.context}{path}"
        return f"{self.url}{path}"

    def get_subjects(self) -> List[str]:
        """Get list of all subjects."""
        response = self.session.get(self._get_url("/subjects"))
        response.raise_for_status()
        subjects = response.json()
        logger.info(f"Retrieved {len(subjects)} subjects from registry")
        return subjects

    def get_versions(self, subject: str) -> List[int]:
        """Get all versions for a subject."""
        response = self.session.get(self._get_url(f"/subjects/{subject}/versions"))
        response.raise_for_status()
        versions = response.json()
        logger.debug(f"Retrieved {len(versions)} versions for subject {subject}")
        return versions

    def get_schema(self, subject: str, version: int) -> Dict:
        """Get schema for a specific subject and version."""
        response = self.session.get(
            self._get_url(f"/subjects/{subject}/versions/{version}")
        )
        response.raise_for_status()
        schema_info = response.json()
        logger.debug(f"Retrieved schema for subject {subject} version {version}")
        return schema_info

    def get_all_schemas(self) -> Dict[str, List[Dict]]:
        """Get all schemas with their versions."""
        schemas = {}
        subjects = self.get_subjects()
        
        for subject in subjects:
            versions = self.get_versions(subject)
            schemas[subject] = []
            for version in versions:
                schema_info = self.get_schema(subject, version)
                schemas[subject].append({
                    'version': version,
                    'id': schema_info.get('id'),
                    'schema': schema_info.get('schema')
                })
        
        logger.info(f"Retrieved schemas for {len(schemas)} subjects")
        return schemas

    def register_schema(self, subject: str, schema: str, schema_type: str = "AVRO") -> Dict:
        """Register a new schema version for a subject."""
        payload = {
            "schema": schema,
            "schemaType": schema_type
        }
        
        # Add import mode header if enabled
        headers = {}
        if self.import_mode:
            headers['X-Registry-Import'] = 'true'
        
        response = self.session.post(
            self._get_url(f"/subjects/{subject}/versions"),
            json=payload,
            headers=headers
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Registered new schema version for subject {subject}")
        return result

    def check_schema_compatibility(self, subject: str, schema: str, version: str = "latest") -> bool:
        """Check if a schema is compatible with a specific version."""
        payload = {
            "schema": schema,
            "schemaType": "AVRO"
        }
        response = self.session.post(
            self._get_url(f"/compatibility/subjects/{subject}/versions/{version}"),
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        return result.get("is_compatible", False)

def compare_schemas(source_schemas: Dict, dest_schemas: Dict) -> Tuple[Dict, List[str]]:
    """Compare schemas between source and destination registries."""
    collisions = []
    comparison = {
        'source_only': [],
        'dest_only': [],
        'common': [],
        'id_differences': []
    }

    # Get all unique schema IDs
    source_ids = set()
    dest_ids = set()
    
    for subject, versions in source_schemas.items():
        for version in versions:
            source_ids.add(version['id'])
            if subject not in dest_schemas:
                comparison['source_only'].append(subject)
            elif not any(v['id'] == version['id'] for v in dest_schemas[subject]):
                comparison['id_differences'].append({
                    'subject': subject,
                    'version': version['version'],
                    'source_id': version['id']
                })

    for subject, versions in dest_schemas.items():
        for version in versions:
            dest_ids.add(version['id'])
            if subject not in source_schemas:
                comparison['dest_only'].append(subject)

    # Find common subjects
    common_subjects = set(source_schemas.keys()) & set(dest_schemas.keys())
    comparison['common'] = list(common_subjects)

    # Check for ID collisions
    for subject, versions in source_schemas.items():
        for version in versions:
            if version['id'] in dest_ids:
                collisions.append({
                    'subject': subject,
                    'version': version['version'],
                    'id': version['id']
                })

    logger.info(f"Comparison complete: {len(comparison['common'])} common subjects, "
                f"{len(comparison['source_only'])} source-only subjects, "
                f"{len(comparison['dest_only'])} dest-only subjects")
    if collisions:
        logger.warning(f"Found {len(collisions)} ID collisions")
    else:
        logger.info("No ID collisions found")

    return comparison, collisions

def migrate_schemas(source_client: SchemaRegistryClient, dest_client: SchemaRegistryClient, 
                   dry_run: bool = True) -> Dict[str, List[Dict]]:
    """Migrate schemas from source to destination registry."""
    migration_results = {
        'successful': [],
        'failed': [],
        'skipped': []
    }

    # Get schemas from both registries
    source_schemas = source_client.get_all_schemas()
    dest_schemas = dest_client.get_all_schemas()

    # Process each subject in source registry
    for subject, versions in source_schemas.items():
        logger.info(f"Processing subject: {subject}")
        
        # Sort versions to ensure we migrate in order
        versions.sort(key=lambda x: x['version'])
        
        for version_info in versions:
            version = version_info['version']
            schema = version_info['schema']
            
            # Check if schema already exists in destination
            if subject in dest_schemas:
                dest_versions = dest_schemas[subject]
                if any(v['schema'] == schema for v in dest_versions):
                    logger.info(f"Skipping {subject} version {version} - schema already exists")
                    migration_results['skipped'].append({
                        'subject': subject,
                        'version': version,
                        'reason': 'Schema already exists'
                    })
                    continue

            try:
                if not dry_run:
                    # Register schema in destination
                    result = dest_client.register_schema(subject, schema)
                    logger.info(f"Successfully migrated {subject} version {version}")
                    migration_results['successful'].append({
                        'subject': subject,
                        'version': version,
                        'new_id': result.get('id')
                    })
                else:
                    # In dry run mode, just check compatibility
                    is_compatible = dest_client.check_schema_compatibility(subject, schema)
                    if is_compatible:
                        logger.info(f"[DRY RUN] Would migrate {subject} version {version} - compatible")
                        migration_results['successful'].append({
                            'subject': subject,
                            'version': version,
                            'reason': 'Compatible in dry run'
                        })
                    else:
                        logger.warning(f"[DRY RUN] Would fail to migrate {subject} version {version} - incompatible")
                        migration_results['failed'].append({
                            'subject': subject,
                            'version': version,
                            'reason': 'Incompatible schema'
                        })
            except Exception as e:
                logger.error(f"Failed to migrate {subject} version {version}: {str(e)}")
                migration_results['failed'].append({
                    'subject': subject,
                    'version': version,
                    'reason': str(e)
                })

    return migration_results

def display_results(source_schemas: Dict, dest_schemas: Dict, comparison: Dict, collisions: List[Dict]):
    """Display results using logging."""
    # Log schema statistics
    logger.info("Schema Registry Statistics:")
    logger.info(f"Source Registry: {len(source_schemas)} subjects")
    logger.info(f"Destination Registry: {len(dest_schemas)} subjects")

    # Log comparison results
    logger.info("\nSchema Comparison Results:")
    for category, subjects in comparison.items():
        if isinstance(subjects, list):
            if subjects:
                logger.info(f"\n{category}:")
                for subject in subjects:
                    logger.info(f"  - {subject}")
        else:
            logger.info(f"{category}: {subjects}")

    # Log ID collisions
    if collisions:
        logger.warning("\nID Collisions Found:")
        for collision in collisions:
            logger.warning(
                f"Subject: {collision['subject']}, "
                f"Version: {collision['version']}, "
                f"ID: {collision['id']}"
            )
    else:
        logger.info("\nNo ID collisions found")

def display_migration_results(results: Dict[str, List[Dict]]):
    """Display migration results using logging."""
    logger.info("\nMigration Results:")
    
    # Display successful migrations
    if results['successful']:
        logger.info("\nSuccessful Migrations:")
        for migration in results['successful']:
            logger.info(
                f"Subject: {migration['subject']}, "
                f"Version: {migration['version']}"
                + (f", New ID: {migration['new_id']}" if 'new_id' in migration else "")
            )
    
    # Display failed migrations
    if results['failed']:
        logger.warning("\nFailed Migrations:")
        for migration in results['failed']:
            logger.warning(
                f"Subject: {migration['subject']}, "
                f"Version: {migration['version']}, "
                f"Reason: {migration['reason']}"
            )
    
    # Display skipped migrations
    if results['skipped']:
        logger.info("\nSkipped Migrations:")
        for migration in results['skipped']:
            logger.info(
                f"Subject: {migration['subject']}, "
                f"Version: {migration['version']}, "
                f"Reason: {migration['reason']}"
            )

def cleanup_destination(client: SchemaRegistryClient) -> None:
    """Clean up the destination registry by deleting all subjects."""
    try:
        subjects = client.get_subjects()
        for subject in subjects:
            try:
                response = client.session.delete(f"{client.url}/subjects/{subject}")
                response.raise_for_status()
                logger.info(f"Successfully deleted subject {subject}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to delete subject {subject}: {e}")
                raise
        logger.info("Successfully cleaned up destination registry")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to clean up destination registry: {e}")
        raise

def main():
    # Initialize source client
    source_client = SchemaRegistryClient(
        url=os.getenv('SOURCE_SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
        username=os.getenv('SOURCE_USERNAME'),
        password=os.getenv('SOURCE_PASSWORD'),
        context=os.getenv('SOURCE_CONTEXT')
    )

    # Initialize destination client with import mode
    dest_client = SchemaRegistryClient(
        url=os.getenv('DEST_SCHEMA_REGISTRY_URL', 'http://localhost:8082'),
        username=os.getenv('DEST_USERNAME'),
        password=os.getenv('DEST_PASSWORD'),
        context=os.getenv('DEST_CONTEXT'),
        import_mode=os.getenv('DEST_IMPORT_MODE', 'false').lower() == 'true'
    )

    try:
        # Get schemas from both registries
        source_schemas = source_client.get_all_schemas()
        dest_schemas = dest_client.get_all_schemas()

        # Compare schemas
        comparison, collisions = compare_schemas(source_schemas, dest_schemas)

        # Display comparison results
        display_results(source_schemas, dest_schemas, comparison, collisions)

        # Perform migration if enabled
        if os.getenv('ENABLE_MIGRATION', 'false').lower() == 'true':
            # Clean up destination if enabled
            if os.getenv('CLEANUP_DESTINATION', 'false').lower() == 'true':
                logger.info("\nCleaning up destination registry before migration...")
                cleanup_destination(dest_client)
                # Refresh destination schemas after cleanup
                dest_schemas = dest_client.get_all_schemas()

            # Perform migration (dry run by default)
            dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
            if dry_run:
                logger.info("\nPerforming dry run migration...")
            else:
                logger.info("\nPerforming actual migration...")
            
            migration_results = migrate_schemas(source_client, dest_client, dry_run=dry_run)
            display_migration_results(migration_results)
        else:
            logger.info("\nMigration disabled. Set ENABLE_MIGRATION=true to enable migration.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Schema Registry: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main()) 