#!/usr/bin/env python3

import os
import json
import requests
import time
import logging
import subprocess
from typing import Dict, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys
sys.path.append('..')  # Add parent directory to path
from schema_registry_migrator import SchemaRegistryClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_session_with_retries() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_schemas(url: str) -> Dict[str, List[Dict]]:
    """Get all schemas from a registry."""
    session = create_session_with_retries()
    
    try:
        response = session.get(f"{url}/subjects")
        response.raise_for_status()
        subjects = response.json()
        
        schemas = {}
        for subject in subjects:
            versions_response = session.get(f"{url}/subjects/{subject}/versions")
            versions_response.raise_for_status()
            versions = versions_response.json()
            
            schemas[subject] = []
            for version in versions:
                schema_response = session.get(f"{url}/subjects/{subject}/versions/{version}")
                schema_response.raise_for_status()
                schema_info = schema_response.json()
                schemas[subject].append({
                    'version': version,
                    'id': schema_info.get('id'),
                    'schema': schema_info.get('schema')
                })
        
        return schemas
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get schemas from {url}: {e}")
        raise

def verify_migration(source_url: str, dest_url: str, import_mode: bool = False) -> bool:
    """Verify that schemas were migrated correctly."""
    try:
        source_schemas = get_schemas(source_url)
        dest_schemas = get_schemas(dest_url)
        
        # Check if all subjects were migrated
        if set(source_schemas.keys()) != set(dest_schemas.keys()):
            logger.error("Not all subjects were migrated")
            return False
        
        # Check each subject's versions
        for subject, source_versions in source_schemas.items():
            dest_versions = dest_schemas[subject]
            
            # Check if all versions were migrated
            if len(source_versions) != len(dest_versions):
                logger.error(f"Subject {subject} has different number of versions")
                return False
            
            # Check each version
            for source_version, dest_version in zip(source_versions, dest_versions):
                # In import mode, IDs should be preserved
                if import_mode and source_version['id'] != dest_version['id']:
                    logger.error(f"Subject {subject} version {source_version['version']} has different ID in import mode")
                    return False
                
                # Schema content should be the same
                if source_version['schema'] != dest_version['schema']:
                    logger.error(f"Subject {subject} version {source_version['version']} has different schema")
                    return False
        
        return True
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False

def run_migration(import_mode: bool = False) -> bool:
    """Run the migration script with specified settings."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'true' if import_mode else 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_comparison() -> bool:
    """Run the migration script in comparison-only mode."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'false',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Comparison failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def cleanup_destination(context: str = None):
    """Clean up the destination registry."""
    session = create_session_with_retries()
    try:
        # Construct the base URL
        base_url = 'http://localhost:38082'
        if context:
            base_url = f"{base_url}/contexts/{context}"
        
        # First get all subjects
        response = session.get(f"{base_url}/subjects")
        response.raise_for_status()
        subjects = response.json()
        
        # Delete each subject individually
        for subject in subjects:
            try:
                # First check if subject is in read-only mode and change it if needed
                mode_response = session.get(f"{base_url}/mode/{subject}")
                if mode_response.status_code == 200:
                    mode_data = mode_response.json()
                    if mode_data.get('mode') != 'READWRITE':
                        # Change to READWRITE mode before deletion
                        session.put(f"{base_url}/mode/{subject}", json={'mode': 'READWRITE'})
                        logger.info(f"Changed subject {subject} to READWRITE mode for deletion")
                
                delete_response = session.delete(f"{base_url}/subjects/{subject}")
                delete_response.raise_for_status()
                logger.info(f"Successfully deleted subject {subject}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to delete subject {subject}: {e}")
                raise
        
        logger.info("Successfully cleaned up destination registry")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to clean up destination registry: {e}")
        raise

def run_cleanup_only() -> bool:
    """Run the migration script with cleanup enabled and dry run, so only cleanup is performed."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'true',  # Only cleanup, no migration
        'DEST_IMPORT_MODE': 'false',
        'CLEANUP_DESTINATION': 'true'
    })
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Cleanup-only run failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def verify_cleanup(dest_url: str, max_retries: int = 5, retry_interval: int = 2) -> bool:
    """Verify that the destination registry is empty after cleanup."""
    for i in range(max_retries):
        try:
            session = create_session_with_retries()
            response = session.get(f"{dest_url}/subjects")
            response.raise_for_status()
            subjects = response.json()
            if not subjects:
                logger.info("Destination registry is empty after cleanup")
                return True
            logger.info(f"Destination registry still has subjects, retrying... ({i+1}/{max_retries})")
            time.sleep(retry_interval)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to verify cleanup: {e}")
            return False
    
    logger.error(f"Destination registry still has subjects after cleanup: {subjects}")
    return False

def populate_source():
    """Populate the source registry with test schemas."""
    try:
        result = subprocess.run(
            ['python', 'populate_source.py'],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to populate source registry: {e}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_migration_with_dest_context() -> bool:
    """Run the migration script with destination context but no source context."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'false',
        'DEST_CONTEXT': 'test-context'  # Set destination context
    })
    
    # First ensure the context exists
    session = create_session_with_retries()
    try:
        # For Confluent Schema Registry 7.5.0, we need to use the mode endpoint
        response = session.put(
            'http://localhost:38082/mode',
            json={'mode': 'IMPORT'}
        )
        response.raise_for_status()
        
        # Now create the context using the correct endpoint
        response = session.put(
            'http://localhost:38082/config',
            json={'context': 'test-context', 'mode': 'IMPORT'}
        )
        if response.status_code == 409:  # Context already exists
            logger.info("Context 'test-context' already exists")
        else:
            response.raise_for_status()
            logger.info("Created context 'test-context'")
            
        # Wait for context to be ready by trying to use it
        max_retries = 5
        retry_interval = 2
        for i in range(max_retries):
            try:
                # Try to get subjects in the context
                response = session.get('http://localhost:38082/contexts/test-context/subjects')
                response.raise_for_status()
                logger.info("Context 'test-context' is ready")
                break
            except requests.exceptions.RequestException as e:
                if i < max_retries - 1:
                    logger.info(f"Context not ready yet, retrying... ({i+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Context verification failed after {max_retries} attempts")
                    return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create context: {e}")
        return False
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration with destination context failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def verify_migration_with_dest_context(source_url: str, dest_url: str) -> bool:
    """Verify that schemas were migrated correctly with destination context."""
    try:
        # Get schemas from source (no context)
        source_schemas = get_schemas(source_url)
        
        # Get schemas from destination with context
        session = create_session_with_retries()
        context_url = f"{dest_url}/contexts/test-context"
        
        # Get subjects in the context
        try:
            response = session.get(f"{context_url}/subjects")
            response.raise_for_status()
            subjects = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get subjects from context: {e}")
            return False
        
        dest_schemas = {}
        for subject in subjects:
            try:
                # Get versions for this subject
                versions_response = session.get(f"{context_url}/subjects/{subject}/versions")
                versions_response.raise_for_status()
                versions = versions_response.json()
                
                # Remove context prefix from subject name for comparison
                base_subject = subject.replace(':.test-context:', '')
                dest_schemas[base_subject] = []
                
                for version in versions:
                    # Get schema for this version
                    schema_response = session.get(f"{context_url}/subjects/{subject}/versions/{version}")
                    schema_response.raise_for_status()
                    schema_info = schema_response.json()
                    dest_schemas[base_subject].append({
                        'version': version,
                        'id': schema_info.get('id'),
                        'schema': schema_info.get('schema')
                    })
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to get schema information for subject {subject}: {e}")
                return False
        
        # Check if all subjects were migrated
        if set(source_schemas.keys()) != set(dest_schemas.keys()):
            logger.error(f"Not all subjects were migrated to destination context. Source: {set(source_schemas.keys())}, Dest: {set(dest_schemas.keys())}")
            return False
        
        # Check each subject's versions
        for subject, source_versions in source_schemas.items():
            dest_versions = dest_schemas[subject]
            
            # Check if all versions were migrated
            if len(source_versions) != len(dest_versions):
                logger.error(f"Subject {subject} has different number of versions in destination context. Source: {len(source_versions)}, Dest: {len(dest_versions)}")
                return False
            
            # Check each version
            for source_version, dest_version in zip(source_versions, dest_versions):
                # Schema content should be the same
                if source_version['schema'] != dest_version['schema']:
                    logger.error(f"Subject {subject} version {source_version['version']} has different schema in destination context")
                    return False
        
        return True
    except Exception as e:
        logger.error(f"Verification of migration with destination context failed: {e}")
        return False

def run_migration_with_different_contexts() -> bool:
    """Run the migration script with different source and destination contexts."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'false',
        'SOURCE_CONTEXT': 'source-context',
        'DEST_CONTEXT': 'dest-context'
    })
    
    # First ensure both contexts exist
    session = create_session_with_retries()
    try:
        # Try to set mode to IMPORT, but don't fail if not supported
        try:
            response = session.put(
                'http://localhost:38081/mode',
                json={'mode': 'IMPORT'}
            )
            response.raise_for_status()
            logger.info("Set mode to IMPORT for source registry")
        except requests.exceptions.RequestException as e:
            logger.info(f"Mode endpoint not supported on source registry: {e}")
        
        try:
            response = session.put(
                'http://localhost:38082/mode',
                json={'mode': 'IMPORT'}
            )
            response.raise_for_status()
            logger.info("Set mode to IMPORT for destination registry")
        except requests.exceptions.RequestException as e:
            logger.info(f"Mode endpoint not supported on destination registry: {e}")
        
        # Create source context
        try:
            response = session.put(
                'http://localhost:38081/config',
                json={'context': 'source-context'}
            )
            if response.status_code == 409:  # Context already exists
                logger.info("Context 'source-context' already exists")
            else:
                response.raise_for_status()
                logger.info("Created context 'source-context'")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create source context: {e}")
            return False
        
        # Create destination context
        try:
            response = session.put(
                'http://localhost:38082/config',
                json={'context': 'dest-context'}
            )
            if response.status_code == 409:  # Context already exists
                logger.info("Context 'dest-context' already exists")
            else:
                response.raise_for_status()
                logger.info("Created context 'dest-context'")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create destination context: {e}")
            return False
            
        # Wait for contexts to be ready
        max_retries = 5
        retry_interval = 2
        for i in range(max_retries):
            try:
                # Try to get subjects in both contexts
                response = session.get('http://localhost:38081/contexts/source-context/subjects')
                response.raise_for_status()
                response = session.get('http://localhost:38082/contexts/dest-context/subjects')
                response.raise_for_status()
                logger.info("Both contexts are ready")
                break
            except requests.exceptions.RequestException as e:
                if i < max_retries - 1:
                    logger.info(f"Contexts not ready yet, retrying... ({i+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Context verification failed after {max_retries} attempts")
                    return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create contexts: {e}")
        return False
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration with different contexts failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_migration_with_same_cluster_contexts() -> bool:
    """Run the migration script with different contexts but same cluster."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38081',  # Same cluster
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'false',
        'SOURCE_CONTEXT': 'source-context',
        'DEST_CONTEXT': 'dest-context'
    })
    
    # First ensure both contexts exist
    session = create_session_with_retries()
    try:
        # Try to set mode to IMPORT, but don't fail if not supported
        try:
            response = session.put(
                'http://localhost:38081/mode',
                json={'mode': 'IMPORT'}
            )
            response.raise_for_status()
            logger.info("Set mode to IMPORT for registry")
        except requests.exceptions.RequestException as e:
            logger.info(f"Mode endpoint not supported: {e}")
        
        # Create source context
        try:
            response = session.put(
                'http://localhost:38081/config',
                json={'context': 'source-context'}
            )
            if response.status_code == 409:  # Context already exists
                logger.info("Context 'source-context' already exists")
            else:
                response.raise_for_status()
                logger.info("Created context 'source-context'")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create source context: {e}")
            return False
        
        # Create destination context
        try:
            response = session.put(
                'http://localhost:38081/config',
                json={'context': 'dest-context'}
            )
            if response.status_code == 409:  # Context already exists
                logger.info("Context 'dest-context' already exists")
            else:
                response.raise_for_status()
                logger.info("Created context 'dest-context'")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create destination context: {e}")
            return False
            
        # Wait for contexts to be ready
        max_retries = 5
        retry_interval = 2
        for i in range(max_retries):
            try:
                # Try to get subjects in both contexts
                response = session.get('http://localhost:38081/contexts/source-context/subjects')
                response.raise_for_status()
                response = session.get('http://localhost:38081/contexts/dest-context/subjects')
                response.raise_for_status()
                logger.info("Both contexts are ready")
                break
            except requests.exceptions.RequestException as e:
                if i < max_retries - 1:
                    logger.info(f"Contexts not ready yet, retrying... ({i+1}/{max_retries})")
                    time.sleep(retry_interval)
                else:
                    logger.error(f"Context verification failed after {max_retries} attempts")
                    return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create contexts: {e}")
        return False
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration with same cluster contexts failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def verify_migration_with_different_contexts(source_url: str, dest_url: str) -> bool:
    """Verify that schemas were migrated correctly between different contexts."""
    try:
        # Get schemas from source context
        session = create_session_with_retries()
        source_context_url = f"{source_url}/contexts/source-context"
        dest_context_url = f"{dest_url}/contexts/dest-context"
        
        # Get subjects in source context
        try:
            response = session.get(f"{source_context_url}/subjects")
            response.raise_for_status()
            source_subjects = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get subjects from source context: {e}")
            return False
        
        # Get subjects in destination context
        try:
            response = session.get(f"{dest_context_url}/subjects")
            response.raise_for_status()
            dest_subjects = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get subjects from destination context: {e}")
            return False
        
        # Remove context prefixes for comparison
        source_schemas = {}
        for subject in source_subjects:
            base_subject = subject.replace(':.source-context:', '')
            source_schemas[base_subject] = []
            try:
                versions_response = session.get(f"{source_context_url}/subjects/{subject}/versions")
                versions_response.raise_for_status()
                versions = versions_response.json()
                
                for version in versions:
                    schema_response = session.get(f"{source_context_url}/subjects/{subject}/versions/{version}")
                    schema_response.raise_for_status()
                    schema_info = schema_response.json()
                    source_schemas[base_subject].append({
                        'version': version,
                        'id': schema_info.get('id'),
                        'schema': schema_info.get('schema')
                    })
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to get schema information for subject {subject}: {e}")
                return False
        
        dest_schemas = {}
        for subject in dest_subjects:
            base_subject = subject.replace(':.dest-context:', '')
            dest_schemas[base_subject] = []
            try:
                versions_response = session.get(f"{dest_context_url}/subjects/{subject}/versions")
                versions_response.raise_for_status()
                versions = versions_response.json()
                
                for version in versions:
                    schema_response = session.get(f"{dest_context_url}/subjects/{subject}/versions/{version}")
                    schema_response.raise_for_status()
                    schema_info = schema_response.json()
                    dest_schemas[base_subject].append({
                        'version': version,
                        'id': schema_info.get('id'),
                        'schema': schema_info.get('schema')
                    })
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to get schema information for subject {subject}: {e}")
                return False
        
        # Check if all subjects were migrated
        if set(source_schemas.keys()) != set(dest_schemas.keys()):
            logger.error(f"Not all subjects were migrated between contexts. Source: {set(source_schemas.keys())}, Dest: {set(dest_schemas.keys())}")
            return False
        
        # Check each subject's versions
        for subject, source_versions in source_schemas.items():
            dest_versions = dest_schemas[subject]
            
            # Check if all versions were migrated
            if len(source_versions) != len(dest_versions):
                logger.error(f"Subject {subject} has different number of versions between contexts. Source: {len(source_versions)}, Dest: {len(dest_versions)}")
                return False
            
            # Check each version
            for source_version, dest_version in zip(source_versions, dest_versions):
                # Schema content should be the same
                if source_version['schema'] != dest_version['schema']:
                    logger.error(f"Subject {subject} version {source_version['version']} has different schema between contexts")
                    return False
        
        return True
    except Exception as e:
        logger.error(f"Verification of migration between contexts failed: {e}")
        return False

def run_import_mode_migration() -> bool:
    """Run the migration script in import mode."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'true',
        'CLEANUP_DESTINATION': 'true'  # Need to clean up for import mode to work
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Import mode migration failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_authentication_validation() -> bool:
    """Run authentication validation tests."""
    try:
        # Test case 1: Both username and password provided
        client = SchemaRegistryClient(
            url='http://localhost:38081',
            username='test_user',
            password='test_pass'
        )
        if client.auth != ('test_user', 'test_pass'):
            logger.error("Auth validation failed: username/password not set correctly")
            return False

        # Test case 2: Neither username nor password provided
        client = SchemaRegistryClient(url='http://localhost:38081')
        if client.auth is not None:
            logger.error("Auth validation failed: auth should be None when no credentials provided")
            return False

        # Test case 3: Only username provided
        try:
            SchemaRegistryClient(
                url='http://localhost:38081',
                username='test_user'
            )
            logger.error("Auth validation failed: should raise ValueError when only username provided")
            return False
        except ValueError as e:
            if str(e) != "Both username and password must be provided, or neither":
                logger.error(f"Auth validation failed: unexpected error message: {e}")
                return False

        # Test case 4: Only password provided
        try:
            SchemaRegistryClient(
                url='http://localhost:38081',
                password='test_pass'
            )
            logger.error("Auth validation failed: should raise ValueError when only password provided")
            return False
        except ValueError as e:
            if str(e) != "Both username and password must be provided, or neither":
                logger.error(f"Auth validation failed: unexpected error message: {e}")
                return False

        logger.info("All authentication validation tests passed")
        return True
    except Exception as e:
        logger.error(f"Authentication validation test failed with unexpected error: {e}")
        return False

def run_id_collision_with_cleanup() -> bool:
    """Run the migration script with ID collisions and CLEANUP_DESTINATION=true."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'CLEANUP_DESTINATION': 'true',
        'DEST_IMPORT_MODE': 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ID collision with cleanup test failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_id_collision_without_cleanup() -> bool:
    """Run the migration script with ID collisions and CLEANUP_DESTINATION=false."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'CLEANUP_DESTINATION': 'false',
        'DEST_IMPORT_MODE': 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,  # We expect this to fail
            capture_output=True,
            text=True
        )
        if result.returncode == 1:  # Expected failure due to ID collision
            logger.info("ID collision without cleanup test passed (expected failure)")
            return True
        else:
            logger.error("ID collision without cleanup test failed (unexpected success)")
            logger.error(f"Output: {result.stdout}")
            logger.error(f"Error: {result.stderr}")
            return False
    except subprocess.CalledProcessError as e:
        logger.error(f"ID collision without cleanup test failed with unexpected error: {e}")
        return False

def run_id_collision_with_cleanup_and_import_mode() -> bool:
    """Run the migration script with ID collisions, CLEANUP_DESTINATION=true, and DEST_IMPORT_MODE=true."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'CLEANUP_DESTINATION': 'true',
        'DEST_IMPORT_MODE': 'true'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ID collision with cleanup and import mode test failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def setup_readonly_subject(url: str, subject: str) -> bool:
    """Set up a subject in read-only mode for testing."""
    session = create_session_with_retries()
    try:
        # First create a schema for the subject
        schema = {
            "type": "record",
            "name": "ReadOnlyTest",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "string"}
            ]
        }
        
        # Register the schema
        response = session.post(
            f"{url}/subjects/{subject}/versions",
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        logger.info(f"Created schema for subject {subject}")
        
        # Set the subject to READONLY mode
        response = session.put(
            f"{url}/mode/{subject}",
            json={"mode": "READONLY"}
        )
        response.raise_for_status()
        logger.info(f"Set subject {subject} to READONLY mode")
        
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to set up read-only subject: {e}")
        return False

def verify_subject_mode(url: str, subject: str, expected_mode: str) -> bool:
    """Verify that a subject has the expected mode."""
    session = create_session_with_retries()
    try:
        response = session.get(f"{url}/mode/{subject}")
        if response.status_code == 404:
            # No specific mode set, defaults to READWRITE
            actual_mode = "READWRITE"
        else:
            response.raise_for_status()
            actual_mode = response.json().get('mode', 'READWRITE')
        
        if actual_mode == expected_mode:
            logger.info(f"Subject {subject} has expected mode: {expected_mode}")
            return True
        else:
            logger.error(f"Subject {subject} has mode {actual_mode}, expected {expected_mode}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to verify subject mode: {e}")
        return False

def run_migration_with_readonly_subjects() -> bool:
    """Test migration with subjects in read-only mode."""
    # Clean up destination first to avoid ID collisions
    try:
        cleanup_destination()
    except Exception as e:
        logger.error(f"Failed to clean up before read-only test: {e}")
        return False
    
    # First, set up a read-only subject in the destination
    if not setup_readonly_subject('http://localhost:38082', 'test-readonly'):
        logger.error("Failed to set up read-only subject")
        return False
    
    # Verify the subject is in READONLY mode
    if not verify_subject_mode('http://localhost:38082', 'test-readonly', 'READONLY'):
        return False
    
    # Run migration with retry enabled
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'RETRY_FAILED': 'true',
        'PRESERVE_IDS': 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)
        
        # Check if the output mentions mode changes
        if "changing to READWRITE" in result.stdout and "Restoring subject" in result.stdout:
            logger.info("Migration correctly handled read-only subjects")
        else:
            logger.warning("Migration output doesn't show expected mode handling")
        
        # Verify the subject is back to READONLY mode after migration
        if not verify_subject_mode('http://localhost:38082', 'test-readonly', 'READONLY'):
            logger.error("Subject mode was not restored after migration")
            return False
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration with read-only subjects failed with exit code {e.returncode}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def run_migration_with_preserve_ids() -> bool:
    """Test migration with ID preservation enabled."""
    # Clean up destination first
    cleanup_destination()
    
    # Wait a bit for cleanup to complete
    time.sleep(2)
    
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'PRESERVE_IDS': 'true',
        'DEST_IMPORT_MODE': 'true',  # Required for ID preservation
        'CLEANUP_DESTINATION': 'false'  # Already cleaned up
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,  # Don't raise on non-zero exit
            capture_output=True,
            text=True
        )
        
        logger.info(f"Migration exit code: {result.returncode}")
        logger.info(f"Migration stdout: {result.stdout}")
        if result.stderr:
            logger.error(f"Migration stderr: {result.stderr}")
        
        if result.returncode != 0:
            logger.error("Migration failed")
            return False
        
        # Wait a bit for schemas to be registered
        time.sleep(2)
        
        # Verify that IDs were preserved
        source_schemas = get_schemas('http://localhost:38081')
        dest_schemas = get_schemas('http://localhost:38082')
        
        logger.info(f"Source schemas: {list(source_schemas.keys())}")
        logger.info(f"Dest schemas: {list(dest_schemas.keys())}")
        
        for subject, source_versions in source_schemas.items():
            if subject not in dest_schemas:
                logger.error(f"Subject {subject} not found in destination")
                return False
            
            dest_versions = dest_schemas[subject]
            for src_ver, dst_ver in zip(source_versions, dest_versions):
                if src_ver['id'] != dst_ver['id']:
                    logger.error(f"ID not preserved for {subject}: source ID {src_ver['id']}, dest ID {dst_ver['id']}")
                    return False
        
        logger.info("All schema IDs were preserved successfully")
        return True
    except Exception as e:
        logger.error(f"Migration with ID preservation test failed with exception: {e}")
        return False

def run_retry_failed_migrations_test() -> bool:
    """Test the retry mechanism for failed migrations."""
    # Clean up destination first
    try:
        cleanup_destination()
        time.sleep(2)  # Wait for cleanup
    except Exception as e:
        logger.error(f"Failed to clean up before retry test: {e}")
        return False
    
    # First, create a subject that will fail initially
    session = create_session_with_retries()
    
    # Create a schema in destination that will cause a failure
    try:
        schema = {
            "type": "record",
            "name": "FailTest",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }
        
        # Register in destination
        response = session.post(
            'http://localhost:38082/subjects/test-fail/versions',
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        
        # Set to READONLY to cause initial failure
        response = session.put(
            'http://localhost:38082/mode/test-fail',
            json={"mode": "READONLY"}
        )
        response.raise_for_status()
        logger.info("Set up test-fail subject in READONLY mode")
        
        # Create a different schema in source to trigger migration
        schema2 = {
            "type": "record",
            "name": "FailTest",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        
        response = session.post(
            'http://localhost:38081/subjects/test-fail/versions',
            json={"schema": json.dumps(schema2)}
        )
        response.raise_for_status()
        logger.info("Created different schema in source for test-fail")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to set up retry test: {e}")
        return False
    
    # Run migration with retry enabled
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'RETRY_FAILED': 'true',
        'PRESERVE_IDS': 'false'
    })
    
    try:
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,  # Don't raise on non-zero exit
            capture_output=True,
            text=True
        )
        
        logger.info(f"Migration exit code: {result.returncode}")
        logger.info(f"Migration stdout length: {len(result.stdout)}")
        
        # Log key parts of the output - check both stdout and stderr
        output = result.stdout + (result.stderr or '')
        
        if "changing to READWRITE" in output:
            logger.info("Found mode change in output")
        if "Retrying failed migrations" in output:
            logger.info("Found retry in output")
        if "test-fail" in output:
            logger.info("Found test-fail subject in output")
            
        # Check if retry was triggered or if mode handling occurred
        if "Retrying failed migrations" in output or "changing to READWRITE" in output:
            logger.info("Retry mechanism or mode handling was triggered as expected")
            
            # Verify the subject mode was restored
            if not verify_subject_mode('http://localhost:38082', 'test-fail', 'READONLY'):
                logger.error("Subject mode was not restored after retry")
                return False
            
            return True
        else:
            # Log more details to understand why
            logger.error("Neither retry mechanism nor mode handling was triggered")
            logger.error(f"Full stdout:\n{result.stdout}")
            if result.stderr:
                logger.error(f"Full stderr:\n{result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Retry test failed with exception: {e}")
        return False

def run_subject_mode_api_test() -> bool:
    """Test the subject mode API methods directly."""
    try:
        client = SchemaRegistryClient(url='http://localhost:38082')
        
        # Test getting mode for a subject without specific mode
        mode = client.get_subject_mode('test-value')
        if mode != 'READWRITE':
            logger.error(f"Expected default mode READWRITE, got {mode}")
            return False
        
        # Test setting mode
        client.set_subject_mode('test-value', 'READONLY')
        mode = client.get_subject_mode('test-value')
        if mode != 'READONLY':
            logger.error(f"Failed to set mode to READONLY, got {mode}")
            return False
        
        # Test changing mode back
        client.set_subject_mode('test-value', 'READWRITE')
        mode = client.get_subject_mode('test-value')
        if mode != 'READWRITE':
            logger.error(f"Failed to set mode back to READWRITE, got {mode}")
            return False
        
        logger.info("Subject mode API tests passed")
        return True
        
    except Exception as e:
        logger.error(f"Subject mode API test failed: {e}")
        return False

def run_json_schema_migration_test() -> bool:
    """Test migration of JSON schemas."""
    # Clean up destination first
    try:
        cleanup_destination()
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to clean up before JSON schema test: {e}")
        return False
    
    session = create_session_with_retries()
    
    try:
        # Create a JSON schema in source
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["id", "name"]
        }
        
        # Register JSON schema in source
        response = session.post(
            'http://localhost:38081/subjects/test-json-subject/versions',
            json={
                "schema": json.dumps(json_schema),
                "schemaType": "JSON"
            }
        )
        response.raise_for_status()
        logger.info("Created JSON schema in source registry")
        
        # Run migration
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false'
        })
        
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Migration failed with exit code {result.returncode}")
            logger.error(f"Stderr: {result.stderr}")
            return False
        
        # Verify the schema was migrated with correct type
        response = session.get('http://localhost:38082/subjects/test-json-subject/versions/1')
        if response.status_code == 200:
            schema_info = response.json()
            if schema_info.get('schemaType') == 'JSON':
                logger.info("JSON schema migrated successfully with correct type")
                return True
            else:
                logger.error(f"Schema type mismatch: expected JSON, got {schema_info.get('schemaType')}")
                return False
        else:
            logger.error("Failed to verify migrated JSON schema")
            return False
            
    except Exception as e:
        logger.error(f"JSON schema migration test failed: {e}")
        return False

def run_protobuf_schema_migration_test() -> bool:
    """Test migration of PROTOBUF schemas."""
    # Clean up destination first
    try:
        cleanup_destination()
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to clean up before PROTOBUF schema test: {e}")
        return False
    
    session = create_session_with_retries()
    
    try:
        # Create a simple Protobuf schema
        protobuf_schema = '''
syntax = "proto3";

message TestMessage {
  int32 id = 1;
  string name = 2;
  string email = 3;
}
'''
        
        # Register PROTOBUF schema in source
        response = session.post(
            'http://localhost:38081/subjects/test-protobuf-subject/versions',
            json={
                "schema": protobuf_schema,
                "schemaType": "PROTOBUF"
            }
        )
        response.raise_for_status()
        logger.info("Created PROTOBUF schema in source registry")
        
        # Run migration
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false'
        })
        
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Migration failed with exit code {result.returncode}")
            logger.error(f"Stderr: {result.stderr}")
            return False
        
        # Verify the schema was migrated with correct type
        response = session.get('http://localhost:38082/subjects/test-protobuf-subject/versions/1')
        if response.status_code == 200:
            schema_info = response.json()
            if schema_info.get('schemaType') == 'PROTOBUF':
                logger.info("PROTOBUF schema migrated successfully with correct type")
                return True
            else:
                logger.error(f"Schema type mismatch: expected PROTOBUF, got {schema_info.get('schemaType')}")
                return False
        else:
            logger.error("Failed to verify migrated PROTOBUF schema")
            return False
            
    except Exception as e:
        logger.error(f"PROTOBUF schema migration test failed: {e}")
        return False

def run_mixed_schema_types_test() -> bool:
    """Test migration with multiple schema types in one run."""
    # Clean up destination first
    try:
        cleanup_destination()
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to clean up before mixed schema types test: {e}")
        return False
    
    session = create_session_with_retries()
    
    try:
        # Create AVRO schema
        avro_schema = {
            "type": "record",
            "name": "MixedTest",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "string"}
            ]
        }
        
        # Create JSON schema
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "data": {"type": "string"}
            }
        }
        
        # Create PROTOBUF schema
        protobuf_schema = '''
syntax = "proto3";

message MixedTest {
  int32 id = 1;
  string data = 2;
}
'''
        
        # Register all schemas in source
        schemas = [
            ("test-mixed-avro", json.dumps(avro_schema), "AVRO"),
            ("test-mixed-json", json.dumps(json_schema), "JSON"),
            ("test-mixed-protobuf", protobuf_schema, "PROTOBUF")
        ]
        
        for subject, schema, schema_type in schemas:
            response = session.post(
                f'http://localhost:38081/subjects/{subject}/versions',
                json={
                    "schema": schema,
                    "schemaType": schema_type
                }
            )
            response.raise_for_status()
            logger.info(f"Created {schema_type} schema for subject {subject}")
        
        # Run migration
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false'
        })
        
        result = subprocess.run(
            ['python', '../schema_registry_migrator.py'],
            env=env,
            check=False,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Migration failed with exit code {result.returncode}")
            logger.error(f"Stderr: {result.stderr}")
            return False
        
        # Verify all schemas were migrated with correct types
        all_correct = True
        for subject, _, expected_type in schemas:
            response = session.get(f'http://localhost:38082/subjects/{subject}/versions/1')
            if response.status_code == 200:
                schema_info = response.json()
                actual_type = schema_info.get('schemaType', 'AVRO')
                if actual_type == expected_type:
                    logger.info(f"Schema {subject} migrated with correct type: {expected_type}")
                else:
                    logger.error(f"Schema type mismatch for {subject}: expected {expected_type}, got {actual_type}")
                    all_correct = False
            else:
                logger.error(f"Failed to verify migrated schema for {subject}")
                all_correct = False
        
        return all_correct
            
    except Exception as e:
        logger.error(f"Mixed schema types test failed: {e}")
        return False

def main():
    """Run all tests."""
    tests = [
        (1, "Comparison-only test", run_comparison),
        (2, "Cleanup test", run_cleanup_only),
        (3, "Normal migration test", run_migration),
        (4, "Import mode migration test", run_import_mode_migration),
        (5, "Context migration test", run_migration_with_different_contexts),
        (6, "Same cluster context migration test", run_migration_with_same_cluster_contexts),
        (7, "Authentication validation test", run_authentication_validation),
        (8, "ID collision with cleanup test", run_id_collision_with_cleanup),
        (9, "ID collision without cleanup test", run_id_collision_without_cleanup),
        (10, "ID collision with cleanup and import mode test", run_id_collision_with_cleanup_and_import_mode),
        (11, "Subject mode API test", run_subject_mode_api_test),
        (12, "Migration with read-only subjects test", run_migration_with_readonly_subjects),
        (13, "Migration with ID preservation test", run_migration_with_preserve_ids),
        (14, "Retry failed migrations test", run_retry_failed_migrations_test),
        (15, "JSON schema migration test", run_json_schema_migration_test),
        (16, "PROTOBUF schema migration test", run_protobuf_schema_migration_test),
        (17, "Mixed schema types test", run_mixed_schema_types_test)
    ]

    success = True
    for test_num, test_name, test_func in tests:
        logger.info(f"\nRunning Test {test_num}: {test_name}...")
        if not test_func():
            logger.error(f"Test {test_num}: {test_name} failed")
            success = False
        else:
            logger.info(f"Test {test_num}: {test_name} passed")

    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 