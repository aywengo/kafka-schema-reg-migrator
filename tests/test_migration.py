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

def test_auth_validation():
    """Test username/password validation."""
    # Test case 1: Both username and password provided
    try:
        client = SchemaRegistryClient(
            url='http://localhost:38081',
            username='test_user',
            password='test_pass'
        )
        assert client.auth == ('test_user', 'test_pass')
    except ValueError as e:
        assert False, f"Should not raise ValueError: {e}"

    # Test case 2: Neither username nor password provided
    try:
        client = SchemaRegistryClient(url='http://localhost:38081')
        assert client.auth is None
    except ValueError as e:
        assert False, f"Should not raise ValueError: {e}"

    # Test case 3: Only username provided
    try:
        SchemaRegistryClient(
            url='http://localhost:38081',
            username='test_user'
        )
        assert False, "Should raise ValueError"
    except ValueError as e:
        assert str(e) == "Both username and password must be provided, or neither"

    # Test case 4: Only password provided
    try:
        SchemaRegistryClient(
            url='http://localhost:38081',
            password='test_pass'
        )
        assert False, "Should raise ValueError"
    except ValueError as e:
        assert str(e) == "Both username and password must be provided, or neither"

def main():
    # Test auth validation
    logger.info("Testing authentication validation...")
    test_auth_validation()
    logger.info("Authentication validation tests passed")

    # Test 1: Comparison only
    logger.info("Test 1: Comparison only")
    if not run_comparison():
        logger.error("Comparison test failed")
        return 1
    
    logger.info("Comparison test passed")
    
    # Populate source registry for remaining tests
    logger.info("Populating source registry...")
    if not populate_source():
        logger.error("Failed to populate source registry")
        return 1
    
    # Test 2: Cleanup test
    logger.info("Test 2: Cleanup test")
    if not run_cleanup_only():
        logger.error("Cleanup test failed")
        return 1
    
    if not verify_cleanup('http://localhost:38082'):
        logger.error("Cleanup verification failed")
        return 1
    
    logger.info("Cleanup test passed")
    
    # Test 3: Normal migration
    logger.info("Test 3: Normal migration")
    if not run_migration(import_mode=False):
        logger.error("Normal migration failed")
        return 1
    
    if not verify_migration('http://localhost:38081', 'http://localhost:38082', import_mode=False):
        logger.error("Normal migration verification failed")
        return 1
    
    logger.info("Normal migration test passed")
    
    # Clean up destination registry
    try:
        cleanup_destination()
    except Exception as e:
        logger.error(f"Failed to clean up destination registry: {e}")
        return 1
    
    # Test 4: Import mode migration
    logger.info("Test 4: Import mode migration")
    if not run_migration(import_mode=True):
        logger.error("Import mode migration failed")
        return 1
    
    if not verify_migration('http://localhost:38081', 'http://localhost:38082', import_mode=True):
        logger.error("Import mode migration verification failed")
        return 1
    
    logger.info("Import mode migration test passed")
    
    # Test 5: Migration with destination context
    logger.info("Test 5: Migration with destination context")
    try:
        # Clean up both the context and the default context
        cleanup_destination('test-context')
        cleanup_destination()  # Clean up default context as well
    except Exception as e:
        logger.error(f"Failed to clean up destination registry: {e}")
        return 1
    
    if not run_migration_with_dest_context():
        logger.error("Migration with destination context test failed")
        return 1
    
    if not verify_migration_with_dest_context('http://localhost:38081', 'http://localhost:38082'):
        logger.error("Migration with destination context verification failed")
        return 1
    
    logger.info("All tests passed successfully")
    return 0

if __name__ == "__main__":
    exit(main()) 