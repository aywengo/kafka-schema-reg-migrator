#!/usr/bin/env python3

import os
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
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
        context: Optional[str] = None
    ):
        # Validate username and password
        if (username is None) != (password is None):
            raise ValueError("Both username and password must be provided, or neither")
            
        self.url = url.rstrip('/')
        self.auth = (username, password) if username and password else None
        self.context = context
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        logger.info(f"Initialized SchemaRegistryClient for {url}")

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

    def get_latest_version(self, subject: str) -> Optional[int]:
        """Get the latest version number for a subject."""
        try:
            response = self.session.get(self._get_url(f"/subjects/{subject}/versions/latest"))
            response.raise_for_status()
            schema_info = response.json()
            return schema_info.get('version')
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Subject doesn't exist
                return None
            raise

    def get_schema(self, subject: str, version: int) -> Dict:
        """Get schema for a specific subject and version."""
        response = self.session.get(
            self._get_url(f"/subjects/{subject}/versions/{version}")
        )
        response.raise_for_status()
        schema_info = response.json()
        logger.debug(f"Retrieved schema for subject {subject} version {version}")
        return schema_info

    def get_subject_schemas(self, subject: str) -> List[Dict]:
        """Get all schemas for a specific subject."""
        try:
            versions = self.get_versions(subject)
            schemas = []
            for version in versions:
                schema_info = self.get_schema(subject, version)
                schemas.append({
                    'version': version,
                    'id': schema_info.get('id'),
                    'schema': schema_info.get('schema'),
                    'schemaType': schema_info.get('schemaType', 'AVRO')
                })
            return schemas
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Subject doesn't exist
                return []
            raise

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
                    'schema': schema_info.get('schema'),
                    'schemaType': schema_info.get('schemaType', 'AVRO')
                })
        
        logger.info(f"Retrieved schemas for {len(schemas)} subjects")
        return schemas

    def get_subject_mode(self, subject: str) -> str:
        """Get the mode for a specific subject."""
        try:
            response = self.session.get(self._get_url(f"/mode/{subject}"))
            response.raise_for_status()
            result = response.json()
            mode = result.get('mode', 'READWRITE')
            logger.debug(f"Subject {subject} mode: {mode}")
            return mode
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Subject mode not set, defaults to READWRITE
                logger.debug(f"Subject {subject} has no specific mode set, defaulting to READWRITE")
                return 'READWRITE'
            raise

    def set_subject_mode(self, subject: str, mode: str) -> Dict:
        """Set the mode for a specific subject."""
        payload = {"mode": mode}
        response = self.session.put(
            self._get_url(f"/mode/{subject}"),
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Set subject {subject} mode to {mode}")
        return result

    def get_global_mode(self) -> str:
        """Get the global mode for the Schema Registry."""
        try:
            response = self.session.get(self._get_url("/mode"))
            response.raise_for_status()
            result = response.json()
            mode = result.get('mode', 'READWRITE')
            logger.debug(f"Global mode: {mode}")
            return mode
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # No global mode set, defaults to READWRITE
                logger.debug("No global mode set, defaulting to READWRITE")
                return 'READWRITE'
            raise

    def set_global_mode(self, mode: str) -> Dict:
        """Set the global mode for the Schema Registry."""
        payload = {"mode": mode}
        response = self.session.put(
            self._get_url("/mode"),
            json=payload
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Set global mode to {mode}")
        return result

    def get_global_compatibility(self) -> str:
        """Get the global compatibility level for the Schema Registry."""
        try:
            response = self.session.get(self._get_url("/config"))
            response.raise_for_status()
            result = response.json()
            compatibility = result.get('compatibilityLevel', 'BACKWARD')
            logger.debug(f"Global compatibility: {compatibility}")
            return compatibility
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # No global compatibility set, defaults to BACKWARD
                logger.debug("No global compatibility set, defaulting to BACKWARD")
                return 'BACKWARD'
            raise

    def set_global_compatibility(self, compatibility: str) -> Dict:
        """Set the global compatibility level for the Schema Registry."""
        payload = {"compatibility": compatibility}
        response = self.session.put(
            self._get_url("/config"),
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Set global compatibility to {compatibility}")
        return result

    def get_subject_compatibility(self, subject: str) -> Optional[str]:
        """Get the compatibility level for a specific subject."""
        try:
            response = self.session.get(self._get_url(f"/config/{subject}"))
            response.raise_for_status()
            result = response.json()
            compatibility = result.get('compatibilityLevel')
            logger.debug(f"Subject {subject} compatibility: {compatibility}")
            return compatibility
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404 or e.response.status_code == 40408:
                # Subject compatibility not set
                logger.debug(f"Subject {subject} has no specific compatibility set")
                return None
            raise

    def set_subject_compatibility(self, subject: str, compatibility: str) -> Dict:
        """Set the compatibility level for a specific subject."""
        payload = {"compatibility": compatibility}
        response = self.session.put(
            self._get_url(f"/config/{subject}"),
            json=payload,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Set subject {subject} compatibility to {compatibility}")
        return result

    def register_schema(self, subject: str, schema: str, schema_type: str = "AVRO", schema_id: Optional[int] = None) -> Dict:
        """Register a new schema version for a subject."""
        payload = {
            "schema": schema,
            "schemaType": schema_type
        }
        
        # Add schema ID if provided
        if schema_id is not None:
            payload["id"] = schema_id
            logger.debug(f"Including ID {schema_id} in payload")
        
        try:
            response = self.session.post(
                self._get_url(f"/subjects/{subject}/versions"),
                json=payload
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Registered new schema version for subject {subject}" + (f" with ID {schema_id}" if schema_id else ""))
            return result
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                # Conflict - check if the schema already exists
                logger.debug(f"Got 409 conflict for {subject}, checking if schema already exists...")
                try:
                    # Check if this exact schema already exists
                    response = self.session.post(
                        self._get_url(f"/subjects/{subject}"),
                        json=payload
                    )
                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"Schema already exists for subject {subject} with ID {result.get('id')}")
                        return result
                except:
                    pass
                # Re-raise the original 409 error
                raise
            elif e.response.status_code == 422:
                # Try without ID if we get a 422 error
                if schema_id is not None and "id" in payload:
                    logger.warning(f"Failed to register with ID, retrying without ID: {e}")
                    del payload["id"]
                    response = self.session.post(
                        self._get_url(f"/subjects/{subject}/versions"),
                        json=payload
                    )
                    response.raise_for_status()
                    result = response.json()
                    logger.info(f"Registered new schema version for subject {subject} (without ID preservation)")
                    return result
            raise

    def check_schema_exists(self, subject: str, schema: str, schema_type: str = "AVRO") -> Optional[Dict]:
        """Check if a schema already exists for a subject and return its info."""
        try:
            payload = {
                "schema": schema,
                "schemaType": schema_type
            }
            response = self.session.post(
                self._get_url(f"/subjects/{subject}"),
                json=payload
            )
            if response.status_code == 200:
                return response.json()
            return None
        except requests.exceptions.RequestException:
            return None

    def check_schema_compatibility(self, subject: str, schema: str, schema_type: str = "AVRO", version: str = "latest") -> bool:
        """Check if a schema is compatible with a specific version."""
        payload = {
            "schema": schema,
            "schemaType": schema_type
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
        'id_differences': [],
        'version_differences': [],
        'schema_differences': []
    }

    # Get all unique schema IDs
    source_ids = set()
    dest_ids = set()
    
    # Check source schemas
    for subject, versions in source_schemas.items():
        for version in versions:
            source_ids.add(version['id'])
            
            # Check if subject exists in destination
            if subject not in dest_schemas:
                comparison['source_only'].append({
                    'subject': subject,
                    'version': version['version'],
                    'id': version['id']
                })
                continue
            
            # Check if version exists in destination
            dest_versions = dest_schemas[subject]
            dest_version = next((v for v in dest_versions if v['version'] == version['version']), None)
            
            if not dest_version:
                comparison['version_differences'].append({
                    'subject': subject,
                    'version': version['version'],
                    'source_id': version['id'],
                    'type': 'missing_in_dest'
                })
            else:
                # Check schema content
                if version['schema'] != dest_version['schema']:
                    comparison['schema_differences'].append({
                        'subject': subject,
                        'version': version['version'],
                        'source_id': version['id'],
                        'dest_id': dest_version['id']
                    })
                
                # Check ID differences
                if version['id'] != dest_version['id']:
                    comparison['id_differences'].append({
                        'subject': subject,
                        'version': version['version'],
                        'source_id': version['id'],
                        'dest_id': dest_version['id']
                    })

    # Check destination schemas for items not in source
    for subject, versions in dest_schemas.items():
        for version in versions:
            dest_ids.add(version['id'])
            
            # Check if subject exists in source
            if subject not in source_schemas:
                comparison['dest_only'].append({
                    'subject': subject,
                    'version': version['version'],
                    'id': version['id']
                })
                continue
            
            # Check if version exists in source
            source_versions = source_schemas[subject]
            source_version = next((v for v in source_versions if v['version'] == version['version']), None)
            
            if not source_version:
                comparison['version_differences'].append({
                    'subject': subject,
                    'version': version['version'],
                    'dest_id': version['id'],
                    'type': 'missing_in_source'
                })

    # Find common subjects
    common_subjects = set(source_schemas.keys()) & set(dest_schemas.keys())
    comparison['common'] = list(common_subjects)

    # Build a map of destination IDs to their schemas for collision detection
    dest_id_to_schema = {}
    for subject, versions in dest_schemas.items():
        for version in versions:
            dest_id_to_schema[version['id']] = {
                'schema': version['schema'],
                'subject': subject,
                'version': version['version']
            }

    # Check for ID collisions - same ID but different schema content
    for subject, versions in source_schemas.items():
        for version in versions:
            source_id = version['id']
            source_schema = version['schema']
            
            # Check if this ID exists in destination
            if source_id in dest_id_to_schema:
                dest_info = dest_id_to_schema[source_id]
                # Only flag as collision if the schemas are different
                if source_schema != dest_info['schema']:
                    collisions.append({
                        'subject': subject,
                        'version': version['version'],
                        'id': source_id,
                        'dest_subject': dest_info['subject'],
                        'dest_version': dest_info['version']
                    })

    # Log detailed comparison results
    logger.info(f"Comparison complete:")
    logger.info(f"- Common subjects: {len(comparison['common'])}")
    logger.info(f"- Source-only subjects: {len(comparison['source_only'])}")
    logger.info(f"- Destination-only subjects: {len(comparison['dest_only'])}")
    logger.info(f"- Version differences: {len(comparison['version_differences'])}")
    logger.info(f"- Schema differences: {len(comparison['schema_differences'])}")
    logger.info(f"- ID differences: {len(comparison['id_differences'])}")
    
    if collisions:
        logger.warning(f"Found {len(collisions)} ID collisions")
    else:
        logger.info("No ID collisions found")

    return comparison, collisions

def migrate_schemas(source_client: SchemaRegistryClient, dest_client: SchemaRegistryClient, 
                   dry_run: bool = True, preserve_ids: bool = False) -> Dict[str, List[Dict]]:
    """Migrate schemas from source to destination registry."""
    migration_results = {
        'successful': [],
        'failed': [],
        'skipped': []
    }

    # Get schemas from both registries
    source_schemas = source_client.get_all_schemas()
    dest_schemas = dest_client.get_all_schemas()

    # Check if we should automatically handle compatibility issues
    auto_handle_compatibility = os.getenv('AUTO_HANDLE_COMPATIBILITY', 'true').lower() == 'true'

    # Track subjects that need compatibility disabled
    subjects_needing_compatibility_disabled = set()

    # Process each subject in source registry
    for subject, versions in source_schemas.items():
        logger.info(f"Processing subject: {subject}")
        
        # Sort versions to ensure we migrate in order
        versions.sort(key=lambda x: x['version'])
        
        for version_info in versions:
            version = version_info['version']
            schema = version_info['schema']
            schema_id = version_info['id'] if preserve_ids else None
            schema_type = version_info.get('schemaType', 'AVRO')
            
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
                    # First check if this exact schema already exists
                    existing_schema = dest_client.check_schema_exists(subject, schema, schema_type)
                    if existing_schema:
                        logger.info(f"Schema already exists for {subject} with ID {existing_schema.get('id')}, skipping")
                        migration_results['skipped'].append({
                            'subject': subject,
                            'version': version,
                            'existing_id': existing_schema.get('id'),
                            'reason': 'Exact schema already registered'
                        })
                        continue
                    
                    # Check and update subject mode if needed
                    subject_mode = dest_client.get_subject_mode(subject)
                    mode_changed = False
                    
                    # If preserving IDs, we need to set the subject to IMPORT mode
                    if preserve_ids and schema_id is not None:
                        if subject_mode != 'IMPORT':
                            logger.info(f"Setting subject {subject} to IMPORT mode for ID preservation")
                            try:
                                dest_client.set_subject_mode(subject, 'IMPORT')
                                mode_changed = True
                            except requests.exceptions.HTTPError as e:
                                if e.response.status_code == 422:
                                    # Subject must be empty or non-existent to set IMPORT mode
                                    logger.warning(f"Cannot set IMPORT mode for {subject} (subject must be empty): {e}")
                                    # Fall back to migration without ID preservation
                                    schema_id = None
                                    preserve_ids = False
                                else:
                                    raise
                    elif subject_mode != 'READWRITE':
                        logger.info(f"Subject {subject} is in {subject_mode} mode, changing to READWRITE")
                        dest_client.set_subject_mode(subject, 'READWRITE')
                        mode_changed = True
                    
                    try:
                        # Register schema in destination with correct schema type
                        result = dest_client.register_schema(subject, schema, schema_type=schema_type, schema_id=schema_id)
                        logger.info(f"Successfully migrated {subject} version {version} (type: {schema_type})")
                        migration_results['successful'].append({
                            'subject': subject,
                            'version': version,
                            'new_id': result.get('id'),
                            'original_id': version_info['id']
                        })
                    finally:
                        # Restore original mode if it was changed
                        if mode_changed:
                            logger.info(f"Restoring subject {subject} mode to {subject_mode}")
                            dest_client.set_subject_mode(subject, subject_mode)
                else:
                    # In dry run mode, just check compatibility
                    is_compatible = dest_client.check_schema_compatibility(subject, schema, schema_type=schema_type)
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
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 409:
                    # Conflict - might be due to compatibility issues
                    logger.warning(f"Conflict for {subject} version {version}, checking if it's a compatibility issue...")
                    
                    # Check if we should try with compatibility disabled
                    if not dry_run and auto_handle_compatibility and subject not in subjects_needing_compatibility_disabled:
                        subjects_needing_compatibility_disabled.add(subject)
                        logger.info(f"Will retry {subject} with compatibility disabled")
                        migration_results['failed'].append({
                            'subject': subject,
                            'version': version,
                            'reason': '409 Conflict - will retry with compatibility disabled',
                            'retry_with_compatibility_disabled': True
                        })
                    else:
                        # Get the latest version to provide more context
                        latest_version = dest_client.get_latest_version(subject)
                        logger.error(f"409 Conflict for {subject}: schema content differs from existing versions. Latest version in destination: {latest_version}")
                        
                        # Use enhanced error reporting
                        try:
                            comparison = compare_schema_versions(source_client, dest_client, subject, version)
                            if comparison['differences']:
                                logger.error(f"Schema differences for {subject} version {version}:")
                                for diff in comparison['differences']:
                                    logger.error(f"  - {diff}")
                        except Exception as comp_error:
                            logger.debug(f"Could not compare schemas: {comp_error}")
                        
                        migration_results['failed'].append({
                            'subject': subject,
                            'version': version,
                            'reason': f'409 Conflict: Different schema already exists (latest version: {latest_version})'
                        })
                else:
                    logger.error(f"Failed to migrate {subject} version {version}: {str(e)}")
                    migration_results['failed'].append({
                        'subject': subject,
                        'version': version,
                        'reason': str(e)
                    })
            except Exception as e:
                logger.error(f"Failed to migrate {subject} version {version}: {str(e)}")
                migration_results['failed'].append({
                    'subject': subject,
                    'version': version,
                    'reason': str(e)
                })

    # Retry subjects that need compatibility disabled
    if not dry_run and subjects_needing_compatibility_disabled:
        logger.info(f"\nRetrying {len(subjects_needing_compatibility_disabled)} subjects with compatibility disabled...")
        
        for subject in subjects_needing_compatibility_disabled:
            # Store original settings
            original_mode = None
            original_compatibility = None
            mode_changed = False
            
            # Get current compatibility
            original_compatibility = dest_client.get_subject_compatibility(subject)
            if original_compatibility is None:
                # No subject-level compatibility, get global
                original_compatibility = dest_client.get_global_compatibility()
                compatibility_was_global = True
            else:
                compatibility_was_global = False
            
            try:
                # First ensure subject is in READWRITE mode
                try:
                    original_mode = dest_client.get_subject_mode(subject)
                    if original_mode != 'READWRITE':
                        logger.info(f"Setting subject {subject} to READWRITE mode (was {original_mode})")
                        dest_client.set_subject_mode(subject, 'READWRITE')
                        mode_changed = True
                except Exception as e:
                    logger.warning(f"Could not get/set mode for {subject}: {e}")
                
                # Set compatibility to NONE
                logger.info(f"Setting {subject} compatibility to NONE (was {original_compatibility})")
                dest_client.set_subject_compatibility(subject, 'NONE')
                
                # Retry migration for this subject
                subject_versions = source_schemas[subject]
                subject_versions.sort(key=lambda x: x['version'])
                
                for version_info in subject_versions:
                    version = version_info['version']
                    schema = version_info['schema']
                    schema_id = version_info['id'] if preserve_ids else None
                    schema_type = version_info.get('schemaType', 'AVRO')
                    
                    # Skip if already successful
                    if any(m['subject'] == subject and m['version'] == version for m in migration_results['successful']):
                        continue
                    
                    try:
                        # Check if schema already exists
                        existing_schema = dest_client.check_schema_exists(subject, schema, schema_type)
                        if existing_schema:
                            logger.info(f"Schema already exists for {subject} version {version}, skipping")
                            # Remove from failed and add to skipped
                            migration_results['failed'] = [f for f in migration_results['failed'] 
                                                         if not (f['subject'] == subject and f['version'] == version)]
                            migration_results['skipped'].append({
                                'subject': subject,
                                'version': version,
                                'existing_id': existing_schema.get('id'),
                                'reason': 'Exact schema already registered'
                            })
                            continue
                        
                        # For ID preservation, set IMPORT mode if needed
                        import_mode_set = False
                        if preserve_ids and schema_id is not None:
                            try:
                                # Get current mode
                                current_mode = dest_client.get_subject_mode(subject)
                                if current_mode != 'IMPORT':
                                    logger.info(f"Setting subject {subject} to IMPORT mode for ID preservation")
                                    dest_client.set_subject_mode(subject, 'IMPORT')
                                    import_mode_set = True
                            except requests.exceptions.HTTPError as e:
                                if e.response.status_code == 422:
                                    logger.warning(f"Cannot set IMPORT mode for {subject}: {e}")
                                    schema_id = None
                                else:
                                    raise
                        
                        try:
                            # Register schema
                            result = dest_client.register_schema(subject, schema, schema_type=schema_type, schema_id=schema_id)
                            logger.info(f"Successfully migrated {subject} version {version} with compatibility disabled")
                            
                            # Remove from failed and add to successful
                            migration_results['failed'] = [f for f in migration_results['failed'] 
                                                         if not (f['subject'] == subject and f['version'] == version)]
                            migration_results['successful'].append({
                                'subject': subject,
                                'version': version,
                                'new_id': result.get('id'),
                                'original_id': version_info['id'],
                                'compatibility_disabled': True
                            })
                        finally:
                            # Restore mode if we set IMPORT
                            if import_mode_set:
                                try:
                                    dest_client.set_subject_mode(subject, 'READWRITE')
                                except Exception as e:
                                    logger.warning(f"Could not restore mode after IMPORT: {e}")
                                    
                    except Exception as e:
                        logger.error(f"Failed to migrate {subject} version {version} even with compatibility disabled: {e}")
                        # Update the failure reason
                        for failure in migration_results['failed']:
                            if failure['subject'] == subject and failure['version'] == version:
                                failure['reason'] = f"Failed even with compatibility disabled: {str(e)}"
                                break
                
            finally:
                # Restore original mode if it was changed
                if mode_changed and original_mode:
                    try:
                        logger.info(f"Restoring subject {subject} mode to {original_mode}")
                        dest_client.set_subject_mode(subject, original_mode)
                    except Exception as e:
                        logger.warning(f"Could not restore mode for {subject}: {e}")
                
                # Restore original compatibility
                if compatibility_was_global:
                    # Delete subject-level compatibility to revert to global
                    try:
                        response = dest_client.session.delete(dest_client._get_url(f"/config/{subject}"))
                        logger.info(f"Removed subject-level compatibility for {subject}, reverting to global")
                    except:
                        pass
                else:
                    # Restore subject-level compatibility
                    try:
                        dest_client.set_subject_compatibility(subject, original_compatibility)
                        logger.info(f"Restored {subject} compatibility to {original_compatibility}")
                    except Exception as e:
                        logger.warning(f"Could not restore compatibility for {subject}: {e}")

    return migration_results

def retry_failed_migrations(source_client: SchemaRegistryClient, dest_client: SchemaRegistryClient,
                          failed_migrations: List[Dict], preserve_ids: bool = False) -> Dict[str, List[Dict]]:
    """Retry failed migrations with mode changes."""
    retry_results = {
        'successful': [],
        'failed': [],
        'skipped': []
    }
    
    logger.info(f"\nRetrying {len(failed_migrations)} failed migrations...")
    
    # Get source schemas for retry
    source_schemas = source_client.get_all_schemas()
    
    # Get destination schemas to check what already exists
    dest_schemas = dest_client.get_all_schemas()
    
    # Check if we should automatically handle compatibility issues
    auto_handle_compatibility = os.getenv('AUTO_HANDLE_COMPATIBILITY', 'true').lower() == 'true'
    
    # Group failed migrations by subject for efficient processing
    failed_by_subject = {}
    for failed in failed_migrations:
        subject = failed['subject']
        if subject not in failed_by_subject:
            failed_by_subject[subject] = []
        failed_by_subject[subject].append(failed)
    
    # Process each subject
    for subject, subject_failures in failed_by_subject.items():
        logger.info(f"\nRetrying {len(subject_failures)} failed versions for subject: {subject}")
        
        # Store original settings
        original_mode = None
        original_compatibility = None
        mode_changed = False
        compatibility_changed = False
        
        try:
            # Always set subject to READWRITE mode for retry
            try:
                original_mode = dest_client.get_subject_mode(subject)
                if original_mode != 'READWRITE':
                    logger.info(f"Setting subject {subject} to READWRITE mode (was {original_mode})")
                    dest_client.set_subject_mode(subject, 'READWRITE')
                    mode_changed = True
            except Exception as e:
                logger.warning(f"Could not get/set mode for {subject}: {e}")
            
            # Set compatibility to NONE if AUTO_HANDLE_COMPATIBILITY is enabled
            if auto_handle_compatibility:
                try:
                    original_compatibility = dest_client.get_subject_compatibility(subject)
                    if original_compatibility is None:
                        # No subject-level compatibility, get global
                        original_compatibility = dest_client.get_global_compatibility()
                        compatibility_was_global = True
                    else:
                        compatibility_was_global = False
                    
                    logger.info(f"Setting {subject} compatibility to NONE (was {original_compatibility})")
                    dest_client.set_subject_compatibility(subject, 'NONE')
                    compatibility_changed = True
                except Exception as e:
                    logger.warning(f"Could not set compatibility for {subject}: {e}")
            
            # Process each failed version
            for failed in subject_failures:
                version = failed['version']
                
                # Find the schema in source
                if subject not in source_schemas:
                    logger.error(f"Subject {subject} not found in source registry")
                    retry_results['failed'].append({
                        'subject': subject,
                        'version': version,
                        'reason': 'Subject not found in source'
                    })
                    continue
                
                version_info = next((v for v in source_schemas[subject] if v['version'] == version), None)
                if not version_info:
                    logger.error(f"Version {version} not found for subject {subject}")
                    retry_results['failed'].append({
                        'subject': subject,
                        'version': version,
                        'reason': 'Version not found in source'
                    })
                    continue
                
                schema = version_info['schema']
                schema_id = version_info['id'] if preserve_ids else None
                schema_type = version_info.get('schemaType', 'AVRO')
                
                # Check if schema already exists in destination
                if subject in dest_schemas:
                    dest_versions = dest_schemas[subject]
                    if any(v['schema'] == schema for v in dest_versions):
                        logger.info(f"Skipping {subject} version {version} - schema already exists in destination")
                        retry_results['skipped'].append({
                            'subject': subject,
                            'version': version,
                            'reason': 'Schema already exists in destination'
                        })
                        continue
                
                try:
                    # First check if this exact schema already exists
                    existing_schema = dest_client.check_schema_exists(subject, schema, schema_type)
                    if existing_schema:
                        logger.info(f"Schema already exists for {subject} with ID {existing_schema.get('id')}, marking as skipped")
                        retry_results['skipped'].append({
                            'subject': subject,
                            'version': version,
                            'existing_id': existing_schema.get('id'),
                            'reason': 'Exact schema already registered'
                        })
                        continue
                    
                    # For ID preservation, set IMPORT mode if needed
                    import_mode_set = False
                    if preserve_ids and schema_id is not None:
                        try:
                            # Get current mode (might have changed)
                            current_mode = dest_client.get_subject_mode(subject)
                            if current_mode != 'IMPORT':
                                logger.info(f"Setting subject {subject} to IMPORT mode for ID preservation")
                                dest_client.set_subject_mode(subject, 'IMPORT')
                                import_mode_set = True
                        except requests.exceptions.HTTPError as e:
                            if e.response.status_code == 422:
                                logger.warning(f"Cannot set IMPORT mode for {subject}: {e}")
                                schema_id = None
                            else:
                                raise
                    
                    try:
                        # Register schema in destination
                        result = dest_client.register_schema(subject, schema, schema_type=schema_type, schema_id=schema_id)
                        logger.info(f"Successfully migrated {subject} version {version} on retry")
                        retry_results['successful'].append({
                            'subject': subject,
                            'version': version,
                            'new_id': result.get('id'),
                            'original_id': version_info['id']
                        })
                    finally:
                        # Restore mode if we set IMPORT
                        if import_mode_set:
                            try:
                                dest_client.set_subject_mode(subject, 'READWRITE')
                            except Exception as e:
                                logger.warning(f"Could not restore mode after IMPORT: {e}")
                                
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 409:
                        # Conflict - schema might already exist
                        logger.warning(f"Conflict for {subject} version {version}, checking if schema exists...")
                        
                        # Refresh destination schemas
                        try:
                            dest_subject_schemas = dest_client.get_subject_schemas(subject)
                            if any(v['schema'] == schema for v in dest_subject_schemas):
                                logger.info(f"Schema already exists for {subject} version {version}, marking as skipped")
                                retry_results['skipped'].append({
                                    'subject': subject,
                                    'version': version,
                                    'reason': 'Schema already exists (409 conflict)'
                                })
                            else:
                                # Get the latest version to provide more context
                                latest_version = dest_client.get_latest_version(subject)
                                logger.error(f"409 Conflict for {subject}: schema content differs. Latest version: {latest_version}")
                                retry_results['failed'].append({
                                    'subject': subject,
                                    'version': version,
                                    'reason': f'409 Conflict: Different schema exists (latest: {latest_version})'
                                })
                        except Exception as check_error:
                            logger.error(f"Failed to check existing schema: {check_error}")
                            retry_results['failed'].append({
                                'subject': subject,
                                'version': version,
                                'reason': f'409 Conflict and failed to verify: {str(e)}'
                            })
                    else:
                        logger.error(f"Failed to migrate {subject} version {version} on retry: {str(e)}")
                        retry_results['failed'].append({
                            'subject': subject,
                            'version': version,
                            'reason': str(e)
                        })
                except Exception as e:
                    logger.error(f"Failed to migrate {subject} version {version} on retry: {str(e)}")
                    retry_results['failed'].append({
                        'subject': subject,
                        'version': version,
                        'reason': str(e)
                    })
                    
        finally:
            # Restore original settings
            if mode_changed and original_mode:
                try:
                    logger.info(f"Restoring subject {subject} mode to {original_mode}")
                    dest_client.set_subject_mode(subject, original_mode)
                except Exception as e:
                    logger.warning(f"Could not restore mode for {subject}: {e}")
            
            if compatibility_changed:
                try:
                    if compatibility_was_global:
                        # Delete subject-level compatibility to revert to global
                        response = dest_client.session.delete(dest_client._get_url(f"/config/{subject}"))
                        logger.info(f"Removed subject-level compatibility for {subject}, reverting to global")
                    else:
                        # Restore subject-level compatibility
                        dest_client.set_subject_compatibility(subject, original_compatibility)
                        logger.info(f"Restored {subject} compatibility to {original_compatibility}")
                except Exception as e:
                    logger.warning(f"Could not restore compatibility for {subject}: {e}")
    
    return retry_results

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
                f"ID {collision['id']}: "
                f"Source: {collision['subject']} v{collision['version']} <-> "
                f"Dest: {collision['dest_subject']} v{collision['dest_version']}"
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

def cleanup_registry(client: SchemaRegistryClient, permanent: bool = True) -> None:
    """Clean up the destination registry by deleting all subjects.
    
    Args:
        client: The Schema Registry client
        permanent: If True, permanently delete subjects (hard delete). If False, soft delete.
    """
    try:
        subjects = client.get_subjects()
        if not subjects:
            logger.info("No subjects found in registry, nothing to clean up")
            return
            
        for subject in subjects:
            try:
                # First check if subject is in read-only mode and change it if needed
                try:
                    subject_mode = client.get_subject_mode(subject)
                    if subject_mode != 'READWRITE':
                        logger.info(f"Subject {subject} is in {subject_mode} mode, changing to READWRITE for deletion")
                        client.set_subject_mode(subject, 'READWRITE')
                except Exception as e:
                    logger.debug(f"Could not check/change mode for {subject}: {e}")
                
                # Add permanent=true parameter for hard delete
                url = client._get_url(f"/subjects/{subject}")
                
                if permanent:
                    # For permanent delete, we need to do soft delete first
                    try:
                        logger.debug(f"Performing soft delete for subject {subject}")
                        response = client.session.delete(url)
                        response.raise_for_status()
                        logger.debug(f"Soft delete successful for subject {subject}")
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code != 404:
                            logger.debug(f"Soft delete failed with status {e.response.status_code}: {e}")
                    
                    # Now perform hard delete
                    url += "?permanent=true"
                    logger.debug(f"Performing hard delete for subject {subject} with URL: {url}")
                else:
                    logger.debug(f"Performing soft delete for subject {subject} with URL: {url}")
                
                response = client.session.delete(url)
                response.raise_for_status()
                logger.info(f"Successfully {'permanently' if permanent else 'soft'} deleted subject {subject}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Subject {subject} not found (may have been already deleted)")
                    continue
                elif e.response.status_code == 422:
                    # 422 can occur when trying to permanently delete a subject that's in read-only mode
                    # or when the subject has special protections
                    logger.warning(f"Cannot permanently delete subject {subject} (may be protected or in read-only mode)")
                    # Try soft delete instead
                    try:
                        response = client.session.delete(client._get_url(f"/subjects/{subject}"))
                        response.raise_for_status()
                        logger.info(f"Successfully soft deleted subject {subject}")
                    except:
                        logger.warning(f"Could not delete subject {subject}")
                    continue
                logger.error(f"Failed to delete subject {subject}: {e}")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to delete subject {subject}: {e}")
                raise
        logger.info(f"Successfully cleaned up destination registry ({'permanent' if permanent else 'soft'} delete)")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to clean up destination registry: {e}")
        raise

def cleanup_specific_subjects(client: SchemaRegistryClient, subjects_to_clean: List[str], permanent: bool = True) -> None:
    """Clean up specific subjects in the destination registry.
    
    Args:
        client: The Schema Registry client
        subjects_to_clean: List of subject names to delete
        permanent: If True, permanently delete subjects (hard delete). If False, soft delete.
    """
    if not subjects_to_clean:
        logger.info("No subjects specified for cleanup")
        return
        
    logger.info(f"Cleaning up {len(subjects_to_clean)} specific subjects...")
    
    success_count = 0
    failed_subjects = []
    
    for subject in subjects_to_clean:
        try:
            # First check if subject exists by trying to get it from the subjects list
            all_subjects = client.get_subjects()
            logger.debug(f"All subjects in registry: {all_subjects}")
            logger.debug(f"Checking if '{subject}' is in subjects list...")
            if subject not in all_subjects:
                logger.info(f"Subject {subject} not found in subjects list, skipping")
                continue
            else:
                logger.debug(f"Subject '{subject}' found in subjects list, proceeding with deletion")
            
            # Check if subject is in read-only mode and change it if needed
            try:
                subject_mode = client.get_subject_mode(subject)
                if subject_mode != 'READWRITE':
                    logger.info(f"Subject {subject} is in {subject_mode} mode, changing to READWRITE for deletion")
                    client.set_subject_mode(subject, 'READWRITE')
            except Exception as e:
                logger.debug(f"Could not check/change mode for {subject}: {e}")
            
            # Add permanent=true parameter for hard delete
            url = client._get_url(f"/subjects/{subject}")
            
            if permanent:
                # For permanent delete, we need to do soft delete first
                try:
                    logger.debug(f"Performing soft delete for subject {subject}")
                    response = client.session.delete(url)
                    response.raise_for_status()
                    logger.debug(f"Soft delete successful for subject {subject}")
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code != 404:
                        logger.debug(f"Soft delete failed with status {e.response.status_code}: {e}")
                
                # Now perform hard delete
                url += "?permanent=true"
                logger.debug(f"Performing hard delete for subject {subject} with URL: {url}")
            else:
                logger.debug(f"Performing soft delete for subject {subject} with URL: {url}")
            
            response = client.session.delete(url)
            response.raise_for_status()
            logger.info(f"Successfully {'permanently' if permanent else 'soft'} deleted subject {subject}")
            success_count += 1
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Subject {subject} not found (may have been already deleted)")
                continue
            elif e.response.status_code == 422:
                # 422 can occur when trying to permanently delete a subject that's in read-only mode
                logger.warning(f"Cannot permanently delete subject {subject} (may be protected or in read-only mode)")
                # Try soft delete instead
                try:
                    response = client.session.delete(client._get_url(f"/subjects/{subject}"))
                    response.raise_for_status()
                    logger.info(f"Successfully soft deleted subject {subject}")
                    success_count += 1
                except:
                    logger.error(f"Could not delete subject {subject}")
                    failed_subjects.append(subject)
                continue
            logger.error(f"Failed to delete subject {subject}: {e}")
            failed_subjects.append(subject)
        except Exception as e:
            logger.error(f"Failed to delete subject {subject}: {e}")
            failed_subjects.append(subject)
    
    if failed_subjects:
        logger.warning(f"Successfully cleaned {success_count} subjects, failed to clean {len(failed_subjects)} subjects")
        logger.warning(f"Failed subjects: {', '.join(failed_subjects)}")
    else:
        logger.info(f"Successfully cleaned all {success_count} specified subjects")

def compare_schema_versions(source_client: SchemaRegistryClient, dest_client: SchemaRegistryClient, 
                          subject: str, version: int) -> Dict[str, Any]:
    """Compare specific schema versions between source and destination.
    
    Args:
        source_client: Source Schema Registry client
        dest_client: Destination Schema Registry client
        subject: Subject name
        version: Version number to compare
        
    Returns:
        Dictionary with comparison details
    """
    comparison = {
        'subject': subject,
        'version': version,
        'source_exists': False,
        'dest_exists': False,
        'schemas_match': False,
        'source_schema': None,
        'dest_schema': None,
        'differences': []
    }
    
    try:
        # Get source schema
        source_schema_info = source_client.get_schema(subject, version)
        comparison['source_exists'] = True
        comparison['source_schema'] = {
            'id': source_schema_info.get('id'),
            'schema': source_schema_info.get('schema'),
            'schemaType': source_schema_info.get('schemaType', 'AVRO')
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404:
            raise
    
    try:
        # Get destination schema
        dest_schema_info = dest_client.get_schema(subject, version)
        comparison['dest_exists'] = True
        comparison['dest_schema'] = {
            'id': dest_schema_info.get('id'),
            'schema': dest_schema_info.get('schema'),
            'schemaType': dest_schema_info.get('schemaType', 'AVRO')
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404:
            raise
    
    # Compare schemas if both exist
    if comparison['source_exists'] and comparison['dest_exists']:
        source_schema = comparison['source_schema']['schema']
        dest_schema = comparison['dest_schema']['schema']
        
        comparison['schemas_match'] = source_schema == dest_schema
        
        if not comparison['schemas_match']:
            # Try to parse and compare as JSON for better error reporting
            try:
                import json
                source_json = json.loads(source_schema)
                dest_json = json.loads(dest_schema)
                
                # Simple field comparison for AVRO schemas
                if isinstance(source_json, dict) and isinstance(dest_json, dict):
                    source_fields = {f['name'] for f in source_json.get('fields', [])} if 'fields' in source_json else set()
                    dest_fields = {f['name'] for f in dest_json.get('fields', [])} if 'fields' in dest_json else set()
                    
                    fields_only_in_source = source_fields - dest_fields
                    fields_only_in_dest = dest_fields - source_fields
                    
                    if fields_only_in_source:
                        comparison['differences'].append(f"Fields only in source: {', '.join(fields_only_in_source)}")
                    if fields_only_in_dest:
                        comparison['differences'].append(f"Fields only in destination: {', '.join(fields_only_in_dest)}")
                    
                    # Check namespace differences
                    if source_json.get('namespace') != dest_json.get('namespace'):
                        comparison['differences'].append(
                            f"Namespace differs: source='{source_json.get('namespace')}', "
                            f"dest='{dest_json.get('namespace')}'"
                        )
                    
                    # Check type differences
                    if source_json.get('type') != dest_json.get('type'):
                        comparison['differences'].append(
                            f"Type differs: source='{source_json.get('type')}', "
                            f"dest='{dest_json.get('type')}'"
                        )
            except:
                # If parsing fails, just note that schemas differ
                comparison['differences'].append("Schema content differs (unable to parse for detailed comparison)")
    
    return comparison

def set_mode_for_all_subjects(client: SchemaRegistryClient, mode: str) -> None:
    """Set the mode for all subjects in the registry.
    
    Args:
        client: The Schema Registry client
        mode: The mode to set (e.g., 'READWRITE', 'READONLY', 'READWRITE_OVERRIDE')
    """
    try:
        subjects = client.get_subjects()
        if not subjects:
            logger.info("No subjects found in registry to set mode")
            return
        
        logger.info(f"Setting mode to {mode} for {len(subjects)} subjects in destination registry...")
        
        success_count = 0
        failed_subjects = []
        
        for subject in subjects:
            try:
                # Get current mode
                current_mode = client.get_subject_mode(subject)
                
                # Only update if mode is different
                if current_mode != mode:
                    client.set_subject_mode(subject, mode)
                    logger.debug(f"Changed subject {subject} mode from {current_mode} to {mode}")
                    success_count += 1
                else:
                    logger.debug(f"Subject {subject} already in {mode} mode, skipping")
                    success_count += 1
                    
            except Exception as e:
                logger.warning(f"Failed to set mode for subject {subject}: {e}")
                failed_subjects.append(subject)
        
        if failed_subjects:
            logger.warning(f"Successfully set mode for {success_count} subjects, failed for {len(failed_subjects)} subjects")
            logger.warning(f"Failed subjects: {', '.join(failed_subjects)}")
        else:
            logger.info(f"Successfully set mode to {mode} for all {success_count} subjects")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get subjects from registry: {e}")
        raise

def set_global_mode_after_migration(client: SchemaRegistryClient, mode: str) -> None:
    """Set the global mode for the Schema Registry after migration.
    
    Args:
        client: The Schema Registry client
        mode: The mode to set (e.g., 'READWRITE', 'READONLY', 'READWRITE_OVERRIDE')
    """
    try:
        # Get current global mode
        current_mode = client.get_global_mode()
        
        if current_mode != mode:
            logger.info(f"Setting global mode from {current_mode} to {mode}...")
            client.set_global_mode(mode)
            logger.info(f"Successfully set global mode to {mode}")
        else:
            logger.info(f"Global mode already set to {mode}, no change needed")
            
    except Exception as e:
        logger.error(f"Failed to set global mode: {e}")
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
    )

    try:
        # Get schemas from both registries
        source_schemas = source_client.get_all_schemas()
        dest_schemas = dest_client.get_all_schemas()

        # Compare schemas
        comparison, collisions = compare_schemas(source_schemas, dest_schemas)

        # Display comparison results
        display_results(source_schemas, dest_schemas, comparison, collisions)

        # Check for ID collisions before proceeding with migration
        cleanup_destination = os.getenv('CLEANUP_DESTINATION', 'false').lower() == 'true'
        preserve_ids = os.getenv('PRESERVE_IDS', 'false').lower() == 'true'
        
        if collisions:
            if cleanup_destination:
                logger.info("\nID COLLISIONS DETECTED but will be ignored because CLEANUP_DESTINATION=true")
                logger.info("The following schemas have ID conflicts (will be cleaned up):")
                for collision in collisions:
                    logger.info(
                        f"ID {collision['id']}: "
                        f"Source: {collision['subject']} v{collision['version']} <-> "
                        f"Dest: {collision['dest_subject']} v{collision['dest_version']}"
                    )
            else:
                logger.error("\nID COLLISIONS DETECTED! Migration cannot proceed.")
                logger.error("The following schemas have ID conflicts:")
                for collision in collisions:
                    logger.error(
                        f"ID {collision['id']}: "
                        f"Source: {collision['subject']} v{collision['version']} <-> "
                        f"Dest: {collision['dest_subject']} v{collision['dest_version']}"
                    )
                logger.error("\nTo resolve this issue, you can:")
                logger.error("1. Use a different context for the destination registry")
                logger.error("2. Clean up the destination registry first (set CLEANUP_DESTINATION=true)")
                logger.error("3. Manually resolve the ID conflicts")
                logger.error("4. Disable ID preservation (set PRESERVE_IDS=false)")
                return 1

        # Perform migration if enabled
        if os.getenv('ENABLE_MIGRATION', 'false').lower() == 'true':
            # Clean up destination if enabled
            if cleanup_destination:
                logger.info("\nCleaning up destination registry before migration...")
                permanent_delete = os.getenv('PERMANENT_DELETE', 'true').lower() == 'true'
                cleanup_registry(dest_client, permanent=permanent_delete)
                # Refresh destination schemas after cleanup
                dest_schemas = dest_client.get_all_schemas()
            
            # Clean up specific subjects if specified
            cleanup_subjects_env = os.getenv('CLEANUP_SUBJECTS', '')
            if cleanup_subjects_env:
                subjects_to_clean = [s.strip() for s in cleanup_subjects_env.split(',') if s.strip()]
                if subjects_to_clean:
                    logger.info(f"\nCleaning up specific subjects: {', '.join(subjects_to_clean)}")
                    permanent_delete = os.getenv('PERMANENT_DELETE', 'true').lower() == 'true'
                    cleanup_specific_subjects(dest_client, subjects_to_clean, permanent=permanent_delete)
                    # Refresh destination schemas after cleanup
                    dest_schemas = dest_client.get_all_schemas()

            # Perform migration (dry run by default)
            dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
            
            # Set global IMPORT mode AFTER cleanup if requested (only for actual migration, not dry run)
            if not dry_run and os.getenv('DEST_IMPORT_MODE', 'false').lower() == 'true':
                logger.info("\nSetting global mode to IMPORT for destination registry (after cleanup)...")
                try:
                    dest_client.set_global_mode('IMPORT')
                    logger.info("Successfully set global IMPORT mode")
                except Exception as e:
                    logger.warning(f"Failed to set global IMPORT mode: {e}")
                    # Continue anyway, subject-level IMPORT mode will be used for ID preservation
            
            if dry_run:
                logger.info("\nPerforming dry run migration...")
            else:
                logger.info("\nPerforming actual migration...")
                if preserve_ids:
                    logger.info("ID preservation is enabled")
            
            migration_results = migrate_schemas(source_client, dest_client, dry_run=dry_run, preserve_ids=preserve_ids)
            display_migration_results(migration_results)

            # Retry failed migrations if enabled
            if not dry_run and migration_results['failed'] and os.getenv('RETRY_FAILED', 'true').lower() == 'true':
                logger.info("\nRetrying failed migrations with mode changes...")
                retry_results = retry_failed_migrations(source_client, dest_client, 
                                                      migration_results['failed'], 
                                                      preserve_ids=preserve_ids)
                
                # Display retry results
                if retry_results['successful']:
                    logger.info("\nSuccessful Retries:")
                    for migration in retry_results['successful']:
                        logger.info(
                            f"Subject: {migration['subject']}, "
                            f"Version: {migration['version']}, "
                            f"New ID: {migration['new_id']}"
                        )
                
                if retry_results['failed']:
                    logger.warning("\nFailed Retries:")
                    for migration in retry_results['failed']:
                        logger.warning(
                            f"Subject: {migration['subject']}, "
                            f"Version: {migration['version']}, "
                            f"Reason: {migration['reason']}"
                        )
                
                # Update overall results
                migration_results['successful'].extend(retry_results['successful'])
                migration_results['failed'] = retry_results['failed']

            # Validate migration results
            if not dry_run:
                logger.info("\nValidating migration results...")
                # Get updated schemas from both registries
                source_schemas = source_client.get_all_schemas()
                dest_schemas = dest_client.get_all_schemas()
                
                # Check for missing items
                missing_items = []
                for subject, versions in source_schemas.items():
                    if subject not in dest_schemas:
                        missing_items.append({
                            'subject': subject,
                            'reason': 'Subject not found in destination'
                        })
                        continue
                    
                    dest_versions = dest_schemas[subject]
                    for version in versions:
                        if not any(v['schema'] == version['schema'] for v in dest_versions):
                            missing_items.append({
                                'subject': subject,
                                'version': version['version'],
                                'reason': 'Schema version not found in destination'
                            })
                
                if missing_items:
                    logger.warning("\nWARNING: Some items from source are missing in destination:")
                    for item in missing_items:
                        if 'version' in item:
                            logger.warning(f"Subject: {item['subject']}, Version: {item['version']} - {item['reason']}")
                        else:
                            logger.warning(f"Subject: {item['subject']} - {item['reason']}")
                    logger.warning("\nTo resolve missing items, you can:")
                    logger.warning("1. Run the migration again")
                    logger.warning("2. Check the migration logs for any errors")
                    logger.warning("3. Verify that the destination registry is accessible and has sufficient permissions")
                    logger.warning("4. Check if subjects are in read-only mode and need to be changed")
                else:
                    logger.info("Validation successful: All items were migrated correctly")
            
            # Set mode for all subjects after migration if specified
            # This is particularly useful for reverting from IMPORT mode when DEST_IMPORT_MODE=true
            mode_after_migration = os.getenv('DEST_MODE_AFTER_MIGRATION', 'READWRITE')
            if not dry_run:
                logger.info(f"\nSetting global mode in destination registry to {mode_after_migration}...")
                try:
                    set_global_mode_after_migration(dest_client, mode_after_migration)
                except Exception as e:
                    logger.error(f"Failed to set mode after migration: {e}")
                    # Don't fail the entire migration if mode setting fails
                    logger.warning("Migration completed but mode setting failed. You may need to set mode manually.")
            
        else:
            logger.info("\nMigration disabled. Set ENABLE_MIGRATION=true to enable migration.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Schema Registry: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main()) 