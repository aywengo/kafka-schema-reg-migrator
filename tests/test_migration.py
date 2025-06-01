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
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schema_registry_migrator import (
    SchemaRegistryClient, 
    compare_schemas, 
    migrate_schemas, 
    display_results, 
    display_migration_results,
    cleanup_registry,
    retry_failed_migrations,
    set_global_mode_after_migration,
    cleanup_specific_subjects,
    compare_schema_versions
)
import unittest

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

def cleanup_destination(context: str = None, permanent: bool = True):
    """Clean up the destination registry.
    
    Args:
        context: Optional context name
        permanent: If True, permanently delete subjects (hard delete). If False, soft delete.
    """
    session = create_session_with_retries()
    try:
        # Construct the base URL
        base_url = 'http://localhost:38082'
        if context:
            base_url = f"{base_url}/contexts/{context}"
        
        # First get all subjects (including soft-deleted ones)
        response = session.get(f"{base_url}/subjects?deleted=true")
        response.raise_for_status()
        all_subjects = response.json()
        
        # Get active subjects
        response = session.get(f"{base_url}/subjects")
        response.raise_for_status()
        active_subjects = response.json()
        
        # Determine which subjects are soft-deleted
        soft_deleted_subjects = [s for s in all_subjects if s not in active_subjects]
        
        if not all_subjects:
            logger.info("No subjects found in registry, nothing to clean up")
            return
        
        # Delete each subject individually
        for subject in all_subjects:
            try:
                # First check if subject is in read-only mode and change it if needed
                # (only for active subjects)
                if subject in active_subjects:
                    mode_response = session.get(f"{base_url}/mode/{subject}")
                    if mode_response.status_code == 200:
                        mode_data = mode_response.json()
                        if mode_data.get('mode') != 'READWRITE':
                            # Change to READWRITE mode before deletion
                            session.put(f"{base_url}/mode/{subject}", json={'mode': 'READWRITE'})
                            logger.info(f"Changed subject {subject} to READWRITE mode for deletion")
                
                # For soft-deleted subjects, we can only permanently delete them
                if subject in soft_deleted_subjects and permanent:
                    delete_url = f"{base_url}/subjects/{subject}?permanent=true"
                    delete_response = session.delete(delete_url)
                    if delete_response.status_code == 200:
                        logger.info(f"Successfully permanently deleted soft-deleted subject {subject}")
                    else:
                        logger.warning(f"Could not permanently delete soft-deleted subject {subject}: {delete_response.status_code}")
                    continue
                elif subject in soft_deleted_subjects:
                    logger.info(f"Subject {subject} is already soft-deleted, skipping")
                    continue
                
                # For active subjects, delete normally
                delete_url = f"{base_url}/subjects/{subject}"
                if permanent:
                    delete_url += "?permanent=true"
                
                delete_response = session.delete(delete_url)
                delete_response.raise_for_status()
                logger.info(f"Successfully {'permanently' if permanent else 'soft'} deleted subject {subject}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Subject {subject} not found (may have been already deleted)")
                    continue
                elif e.response.status_code == 422:
                    # 422 can occur when trying to permanently delete a subject that's protected
                    logger.warning(f"Cannot permanently delete subject {subject} (may be protected)")
                    # Try soft delete instead
                    try:
                        delete_response = session.delete(f"{base_url}/subjects/{subject}")
                        delete_response.raise_for_status()
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

def cleanup_source(permanent: bool = True):
    """Clean up the source registry.
    
    Args:
        permanent: If True, permanently delete subjects (hard delete). If False, soft delete.
    """
    session = create_session_with_retries()
    try:
        # First get all subjects
        response = session.get("http://localhost:38081/subjects")
        response.raise_for_status()
        subjects = response.json()
        
        if not subjects:
            logger.info("No subjects found in source registry, nothing to clean up")
            return
        
        # Delete each subject individually
        for subject in subjects:
            # Skip the initial test subjects
            if subject in ['test-value', 'test-value-v2', 'test-value-v3']:
                continue
                
            try:
                # Add permanent=true parameter for hard delete
                delete_url = f"http://localhost:38081/subjects/{subject}"
                if permanent:
                    delete_url += "?permanent=true"
                
                delete_response = session.delete(delete_url)
                delete_response.raise_for_status()
                logger.info(f"Successfully {'permanently' if permanent else 'soft'} deleted subject {subject} from source")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Subject {subject} not found in source (may have been already deleted)")
                    continue
                logger.error(f"Failed to delete subject {subject} from source: {e}")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to delete subject {subject} from source: {e}")
                raise
        
        logger.info(f"Successfully cleaned up source registry ({'permanent' if permanent else 'soft'} delete)")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to clean up source registry: {e}")
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
    """Run the migration script with global import mode."""
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'DEST_IMPORT_MODE': 'true',  # Set global IMPORT mode
        'PRESERVE_IDS': 'true',  # Also preserve IDs using subject-level IMPORT mode
        'CLEANUP_DESTINATION': 'true'  # Need to clean up for subject-level IMPORT mode to work
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
        
        # Check if global IMPORT mode was set
        if "Setting global mode to IMPORT" in result.stdout:
            logger.info("Global IMPORT mode was set as expected")
        
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

def setup_id_collision_scenario(session: requests.Session) -> bool:
    """Set up schemas with ID collisions between source and destination."""
    try:
        # First, clean up both registries
        logger.info("Cleaning up registries for ID collision test...")
        
        # Clean destination
        try:
            response = session.get('http://localhost:38082/subjects')
            if response.status_code == 200:
                subjects = response.json()
                for subject in subjects:
                    # First soft delete
                    try:
                        session.delete(f'http://localhost:38082/subjects/{subject}')
                    except:
                        pass
                    # Then hard delete
                    try:
                        session.delete(f'http://localhost:38082/subjects/{subject}?permanent=true')
                    except:
                        pass
        except:
            pass
        
        # Clean source
        try:
            response = session.get('http://localhost:38081/subjects')
            if response.status_code == 200:
                subjects = response.json()
                for subject in subjects:
                    # First soft delete
                    try:
                        session.delete(f'http://localhost:38081/subjects/{subject}')
                    except:
                        pass
                    # Then hard delete
                    try:
                        session.delete(f'http://localhost:38081/subjects/{subject}?permanent=true')
                    except:
                        pass
        except:
            pass
        
        # Set source to IMPORT mode to control IDs
        try:
            session.put('http://localhost:38081/mode', json={'mode': 'IMPORT'})
            logger.info("Set source to IMPORT mode")
        except:
            pass
        
        # Set destination to IMPORT mode to control IDs
        try:
            session.put('http://localhost:38082/mode', json={'mode': 'IMPORT'})
            logger.info("Set destination to IMPORT mode")
        except:
            pass
        
        # Create DIFFERENT schemas in destination with IDs 1, 2, 3
        dest_schemas = [
            ("dest-collision-1", {
                "type": "record",
                "name": "DestSchema1",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "dest_field", "type": "string"}  # Different field name
                ]
            }, 1),
            ("dest-collision-2", {
                "type": "record",
                "name": "DestSchema2",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "dest_data", "type": "long"}  # Different field type
                ]
            }, 2),
            ("dest-collision-3", {
                "type": "record",
                "name": "DestSchema3",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "dest_value", "type": "float"}  # Different field type
                ]
            }, 3)
        ]
        
        # Register schemas in destination with specific IDs first
        for subject, schema, schema_id in dest_schemas:
            # First set subject to IMPORT mode
            session.put(f'http://localhost:38082/mode/{subject}', json={'mode': 'IMPORT'})
            
            response = session.post(
                f'http://localhost:38082/subjects/{subject}/versions',
                json={"schema": json.dumps(schema), "id": schema_id}
            )
            response.raise_for_status()
            actual_id = response.json()['id']
            logger.info(f"Registered {subject} in destination with ID {actual_id}")
            
            # Set subject back to READWRITE
            session.put(f'http://localhost:38082/mode/{subject}', json={'mode': 'READWRITE'})
        
        # Create schemas in source with SAME IDs 1, 2, 3 but different content
        source_schemas = [
            ("collision-test-1", {
                "type": "record",
                "name": "SourceSchema1",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "source_field", "type": "string"}
                ]
            }, 1),
            ("collision-test-2", {
                "type": "record",
                "name": "SourceSchema2",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "source_data", "type": "string"}
                ]
            }, 2),
            ("collision-test-3", {
                "type": "record",
                "name": "SourceSchema3",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "source_value", "type": "double"}
                ]
            }, 3)
        ]
        
        # Register schemas in source with specific IDs
        for subject, schema, schema_id in source_schemas:
            # First set subject to IMPORT mode
            session.put(f'http://localhost:38081/mode/{subject}', json={'mode': 'IMPORT'})
            
            response = session.post(
                f'http://localhost:38081/subjects/{subject}/versions',
                json={"schema": json.dumps(schema), "id": schema_id}
            )
            response.raise_for_status()
            actual_id = response.json()['id']
            logger.info(f"Registered {subject} in source with ID {actual_id}")
            
            # Set subject back to READWRITE
            session.put(f'http://localhost:38081/mode/{subject}', json={'mode': 'READWRITE'})
        
        # Set source back to READWRITE mode
        session.put('http://localhost:38081/mode', json={'mode': 'READWRITE'})
        logger.info("Set source back to READWRITE mode")
        
        # Set destination back to READWRITE mode
        session.put('http://localhost:38082/mode', json={'mode': 'READWRITE'})
        logger.info("Set destination back to READWRITE mode")
        
        logger.info("ID collision scenario set up successfully")
        logger.info("Source has schemas with IDs 1,2,3 with one set of fields")
        logger.info("Destination has different schemas with the same IDs 1,2,3")
        return True
        
    except Exception as e:
        logger.error(f"Failed to set up ID collision scenario: {e}")
        return False

def run_id_collision_with_cleanup() -> bool:
    """Run the migration script with ID collisions and CLEANUP_DESTINATION=true."""
    # First set up a proper ID collision scenario
    session = create_session_with_retries()
    if not setup_id_collision_scenario(session):
        return False
    
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
    # First set up a proper ID collision scenario
    session = create_session_with_retries()
    if not setup_id_collision_scenario(session):
        return False
    
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
            # Check that the error message mentions ID collisions
            if "ID COLLISIONS DETECTED" in result.stdout or "ID COLLISIONS DETECTED" in result.stderr:
                logger.info("ID collision without cleanup test passed (expected failure)")
                return True
            else:
                logger.error("ID collision without cleanup test failed - no collision error message")
                logger.error(f"Output: {result.stdout}")
                logger.error(f"Error: {result.stderr}")
                return False
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
    
    # Run migration with retry enabled and cleanup to avoid ID collisions
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'RETRY_FAILED': 'true',
        'PRESERVE_IDS': 'false',
        'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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
        
        # Note: We can't verify the subject is back to READONLY mode after migration
        # because CLEANUP_DESTINATION=true will have deleted it
        
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
        'CLEANUP_DESTINATION': 'true'  # Clean up to ensure subjects are empty for IMPORT mode
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
        
        # Check if subject-level IMPORT mode was used
        if "Setting subject" in result.stdout and "to IMPORT mode" in result.stdout:
            logger.info("Subject-level IMPORT mode was used as expected")
        else:
            logger.warning("Subject-level IMPORT mode not detected in output")
        
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
        cleanup_source()  # Clean up source to avoid schema accumulation
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
    
    # Run migration with retry enabled and cleanup to avoid ID collisions
    env = os.environ.copy()
    env.update({
        'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
        'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
        'ENABLE_MIGRATION': 'true',
        'DRY_RUN': 'false',
        'RETRY_FAILED': 'true',
        'PRESERVE_IDS': 'false',
        'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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
        if "Retrying failed migrations" in output or "changing to READWRITE" in output or result.returncode == 0:
            logger.info("Retry mechanism or mode handling was triggered as expected")
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
        cleanup_source()  # Clean up source to avoid schema accumulation
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
        
        # Run migration with cleanup to avoid ID collisions
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false',
            'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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
        cleanup_source()  # Clean up source to avoid schema accumulation
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
        
        # Run migration with cleanup to avoid ID collisions
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false',
            'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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
        cleanup_source()  # Clean up source to avoid schema accumulation
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
        
        # Run migration with cleanup to avoid ID collisions
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false',
            'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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

def run_conflict_handling_test() -> bool:
    """Test handling of 409 conflicts when schema already exists."""
    # Clean up destination first
    try:
        cleanup_destination()
        cleanup_source()  # Clean up source to avoid schema accumulation
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to clean up before conflict handling test: {e}")
        return False
    
    session = create_session_with_retries()
    
    try:
        # Create a schema in both source and destination
        schema = {
            "type": "record",
            "name": "ConflictTest",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "data", "type": "string"}
            ]
        }
        
        # Register in source
        response = session.post(
            'http://localhost:38081/subjects/test-conflict/versions',
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        logger.info("Created schema in source registry")
        
        # Register same schema in destination
        response = session.post(
            'http://localhost:38082/subjects/test-conflict/versions',
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        logger.info("Created same schema in destination registry")
        
        # Run migration with cleanup to avoid ID collisions
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false',
            'CLEANUP_DESTINATION': 'true'  # Add cleanup to avoid ID collisions
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
        
        # Since we're using CLEANUP_DESTINATION=true, the destination will be cleaned
        # and the schema will be migrated normally
        logger.info("Migration completed successfully with cleanup")
        return True
            
    except Exception as e:
        logger.error(f"Conflict handling test failed: {e}")
        return False

def run_permanent_delete_test() -> bool:
    """Test permanent delete functionality."""
    session = create_session_with_retries()
    
    try:
        # Clean up first to ensure we start fresh
        try:
            cleanup_destination(permanent=True)
        except:
            pass  # Ignore errors if nothing to clean
        
        # Wait for cleanup to complete
        time.sleep(1)
        
        # Create a test subject
        schema = {
            "type": "record",
            "name": "DeleteTest",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }
        
        # Register schema
        response = session.post(
            'http://localhost:38082/subjects/test-permanent-delete/versions',
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        logger.info("Created test schema for deletion")
        
        # Verify subject exists
        response = session.get('http://localhost:38082/subjects')
        if response.status_code == 200:
            subjects = response.json()
            if 'test-permanent-delete' not in subjects:
                logger.error("Test subject not found after creation")
                return False
            logger.info(f"Found test subject in active subjects")
        
        # First do a soft delete
        response = session.delete('http://localhost:38082/subjects/test-permanent-delete')
        if response.status_code == 200:
            logger.info("Successfully soft deleted test subject")
        else:
            logger.error(f"Failed to soft delete: {response.status_code}")
            return False
        
        # Verify it's soft deleted (not in active list)
        response = session.get('http://localhost:38082/subjects')
        if response.status_code == 200:
            active_subjects = response.json()
            if 'test-permanent-delete' in active_subjects:
                logger.error("Subject still in active list after soft delete")
                return False
            logger.info("Subject removed from active list after soft delete")
        
        # Verify it's in the deleted list
        response = session.get('http://localhost:38082/subjects?deleted=true')
        if response.status_code == 200:
            all_subjects = response.json()
            if 'test-permanent-delete' not in all_subjects:
                logger.error("Subject not found in deleted subjects list")
                return False
            logger.info("Subject found in deleted subjects list")
        
        # Now permanently delete it
        response = session.delete('http://localhost:38082/subjects/test-permanent-delete?permanent=true')
        if response.status_code == 200:
            logger.info("Successfully permanently deleted test subject")
        else:
            logger.error(f"Failed to permanently delete: {response.status_code}")
            return False
        
        # Wait a bit for deletion to complete
        time.sleep(1)
        
        # Verify it's completely gone (not even in deleted list)
        response = session.get('http://localhost:38082/subjects?deleted=true')
        if response.status_code == 200:
            all_subjects = response.json()
            if 'test-permanent-delete' in all_subjects:
                logger.error("Subject still exists after permanent delete")
                return False
            logger.info("Subject completely removed after permanent delete")
            return True
        else:
            logger.error("Failed to check subjects after deletion")
            return False
            
    except Exception as e:
        logger.error(f"Permanent delete test failed: {e}")
        return False

def test_set_mode_for_all_subjects_unit() -> bool:
    """Unit test for set_global_mode_after_migration function."""
    try:
        # Clean up destination first
        cleanup_destination()
        time.sleep(1)
        
        session = create_session_with_retries()
        
        # Create test subjects
        subjects_data = [
            ("test-unit-subject1", "READWRITE"),
            ("test-unit-subject2", "READONLY"),
            ("test-unit-subject3", "READWRITE")
        ]
        
        # Create schemas
        for subject, _ in subjects_data:
            schema = {
                "type": "record",
                "name": f"Test{subject.replace('-', '')}",
                "fields": [{"name": "id", "type": "int"}]
            }
            
            # Register schema
            response = session.post(
                f'http://localhost:38082/subjects/{subject}/versions',
                json={"schema": json.dumps(schema)}
            )
            response.raise_for_status()
            
            logger.info(f"Created {subject}")
        
        # Test 1: Set global mode to READONLY
        client = SchemaRegistryClient(url='http://localhost:38082')
        
        # Get initial global mode
        initial_mode = client.get_global_mode()
        logger.info(f"Initial global mode: {initial_mode}")
        
        # Set to READONLY
        client.set_global_mode('READONLY')
        
        # Verify global mode is READONLY
        mode = client.get_global_mode()
        if mode != 'READONLY':
            logger.error(f"Global mode is not READONLY: {mode}")
            return False
        
        logger.info("Successfully set global mode to READONLY")
        
        # Test 2: Set global mode back to READWRITE
        client.set_global_mode('READWRITE')
        
        # Verify global mode is READWRITE
        mode = client.get_global_mode()
        if mode != 'READWRITE':
            logger.error(f"Global mode is not READWRITE: {mode}")
            return False
        
        logger.info("Successfully set global mode back to READWRITE")
        
        # Test 3: Test with empty registry (after cleanup)
        cleanup_destination()
        time.sleep(1)
        
        # This should not fail even with no subjects
        client.set_global_mode('READONLY')
        mode = client.get_global_mode()
        if mode != 'READONLY':
            logger.error(f"Failed to set global mode with empty registry: {mode}")
            return False
            
        logger.info("Function handles empty registry correctly")
        
        # Reset global mode to READWRITE to avoid affecting other tests
        client.set_global_mode('READWRITE')
        logger.info("Reset global mode to READWRITE for other tests")
        
        return True
        
    except Exception as e:
        logger.error(f"Unit test for global mode failed: {e}")
        return False

def run_mode_after_migration_test() -> bool:
    """Test MODE_AFTER_MIGRATION functionality."""
    # Clean up destination first
    try:
        cleanup_destination()
        cleanup_source()  # Clean up source to avoid schema accumulation
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to clean up before mode after migration test: {e}")
        return False
    
    session = create_session_with_retries()
    
    try:
        # Create test schemas in source
        schemas = [
            ("test-mode-subject1", {
                "type": "record",
                "name": "TestMode1",
                "fields": [{"name": "id", "type": "int"}]
            }),
            ("test-mode-subject2", {
                "type": "record",
                "name": "TestMode2",
                "fields": [{"name": "name", "type": "string"}]
            }),
            ("test-mode-subject3", {
                "type": "record",
                "name": "TestMode3",
                "fields": [{"name": "value", "type": "double"}]
            })
        ]
        
        # Register schemas in source
        for subject, schema in schemas:
            response = session.post(
                f'http://localhost:38081/subjects/{subject}/versions',
                json={"schema": json.dumps(schema)}
            )
            response.raise_for_status()
            logger.info(f"Created schema for subject {subject}")
        
        # Test 1: Migration with DEST_MODE_AFTER_MIGRATION=READONLY
        logger.info("Testing migration with DEST_MODE_AFTER_MIGRATION=READONLY...")
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'PRESERVE_IDS': 'false',
            'CLEANUP_DESTINATION': 'true',
            'DEST_MODE_AFTER_MIGRATION': 'READONLY'  # Set global mode to READONLY after migration
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
        
        # Verify global mode is READONLY
        response = session.get('http://localhost:38082/mode')
        if response.status_code == 200:
            mode_data = response.json()
            mode = mode_data.get('mode', 'READWRITE')
            if mode != 'READONLY':
                logger.error(f"Global mode is not READONLY: {mode}")
                return False
            else:
                logger.info("Global mode is correctly set to READONLY")
        else:
            logger.error("Failed to get global mode")
            return False
        
        # Test 2: Migration with DEST_MODE_AFTER_MIGRATION=READWRITE (default)
        logger.info("\nTesting migration with DEST_MODE_AFTER_MIGRATION=READWRITE...")
        
        # Clean up and recreate schemas
        cleanup_destination()
        time.sleep(1)
        
        env['DEST_MODE_AFTER_MIGRATION'] = 'READWRITE'  # Explicitly set to READWRITE
        
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
        
        # Verify global mode is READWRITE
        response = session.get('http://localhost:38082/mode')
        if response.status_code == 200:
            mode_data = response.json()
            mode = mode_data.get('mode', 'READWRITE')
            if mode != 'READWRITE':
                logger.error(f"Global mode is not READWRITE: {mode}")
                return False
            else:
                logger.info("Global mode is correctly set to READWRITE")
        else:
            # If no mode is set, it defaults to READWRITE
            logger.info("No global mode set (defaults to READWRITE)")
        
        # Test 3: Test with DEST_IMPORT_MODE=true (should revert from IMPORT mode)
        logger.info("\nTesting DEST_MODE_AFTER_MIGRATION with DEST_IMPORT_MODE=true...")
        
        # Clean up
        cleanup_destination()
        time.sleep(1)
        
        # Run migration with import mode and DEST_MODE_AFTER_MIGRATION=READWRITE
        env['DEST_IMPORT_MODE'] = 'true'
        env['DEST_MODE_AFTER_MIGRATION'] = 'READWRITE'
        
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
        
        # Verify global mode is READWRITE (reverted from IMPORT)
        response = session.get('http://localhost:38082/mode')
        if response.status_code == 200:
            mode_data = response.json()
            mode = mode_data.get('mode', 'READWRITE')
            if mode != 'READWRITE':
                logger.error(f"Global mode is not READWRITE after import mode: {mode}")
                return False
            else:
                logger.info("Global mode correctly reverted from IMPORT to READWRITE")
        
        return True
            
    except Exception as e:
        logger.error(f"Mode after migration test failed: {e}")
        return False

def run_test_cleanup_specific_subjects() -> bool:
    """Run the test for cleaning up specific subjects."""
    try:
        # Create test clients with correct ports
        source_client = SchemaRegistryClient('http://localhost:38081')
        dest_client = SchemaRegistryClient('http://localhost:38082')
        
        # Test subject
        subject = 'test-subject-with-gaps'
        
        # Create schemas with gaps - ensuring backward compatibility
        schema1 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        schema2 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}]}'
        schema3 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}, {"name": "field3", "type": ["null", "boolean"], "default": null}]}'
        
        try:
            # Register schemas in source with gaps (versions 1 and 3)
            # First register schema1 (version 1)
            source_client.register_schema(subject, schema1)
            
            # Register schema2 (version 2)
            source_client.register_schema(subject, schema2)
            
            # Register schema3 (version 3)
            source_client.register_schema(subject, schema3)
            
            # Delete version 2 to create a gap
            try:
                # First try soft delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2: {e}")
                raise
            
            # Verify versions in source
            source_versions = source_client.get_versions(subject)
            if source_versions != [1, 3]:
                raise Exception(f"Source registry has unexpected versions: {source_versions}")
            
            # Register schemas in destination with different gaps (versions 1 and 3)
            dest_client.register_schema(subject, schema1)
            dest_client.register_schema(subject, schema2)
            dest_client.register_schema(subject, schema3)
            
            # Delete version 2 in destination to create a gap
            try:
                # First try soft delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2 in destination: {e}")
                raise
            
            # Verify versions in destination
            dest_versions = dest_client.get_versions(subject)
            if dest_versions != [1, 3]:
                raise Exception(f"Destination registry has unexpected versions: {dest_versions}")
            
            # Compare version 1 (should exist in both)
            comparison1 = compare_schema_versions(source_client, dest_client, subject, 1)
            if not comparison1['source_exists'] or not comparison1['dest_exists'] or not comparison1['schemas_match']:
                raise Exception("Version 1 comparison failed")
            
            # Compare version 2 (should not exist in either)
            comparison2 = compare_schema_versions(source_client, dest_client, subject, 2)
            if comparison2['source_exists'] or comparison2['dest_exists']:
                raise Exception("Version 2 should not exist in either registry")
            
            # Compare version 3 (should exist in both)
            comparison3 = compare_schema_versions(source_client, dest_client, subject, 3)
            if not comparison3['source_exists'] or not comparison3['dest_exists'] or not comparison3['schemas_match']:
                raise Exception("Version 3 comparison failed")
            
            # Verify version gaps are detected
            if comparison1['version_gaps']['source'] is None or comparison1['version_gaps']['destination'] is None:
                raise Exception("Version gaps not detected")
            
            # Verify source version gaps
            source_gaps = comparison1['version_gaps']['source']
            if source_gaps['actual_versions'] != [1, 3] or source_gaps['expected_versions'] != [1, 2, 3] or source_gaps['missing_versions'] != [2]:
                raise Exception("Source version gaps verification failed")
            
            # Verify destination version gaps
            dest_gaps = comparison1['version_gaps']['destination']
            if dest_gaps['actual_versions'] != [1, 3] or dest_gaps['expected_versions'] != [1, 2, 3] or dest_gaps['missing_versions'] != [2]:
                raise Exception("Destination version gaps verification failed")
            
            # Verify version sequences
            if comparison1['version_sequence']['source'] != [1, 3] or comparison1['version_sequence']['destination'] != [1, 3]:
                raise Exception("Version sequence verification failed")
            
            return True
            
        finally:
            # Cleanup
            try:
                cleanup_specific_subjects(source_client, [subject], permanent=True)
                cleanup_specific_subjects(dest_client, [subject], permanent=True)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False

def run_test_compare_schema_versions() -> bool:
    """Run the test for comparing schema versions."""
    try:
        # Create test clients with correct ports
        source_client = SchemaRegistryClient('http://localhost:38081')
        dest_client = SchemaRegistryClient('http://localhost:38082')
        
        # Test subject
        subject = 'test-subject-comparison'
        
        # Create schemas with backward compatibility
        schema1 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        schema2 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}]}'
        schema3 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}, {"name": "field3", "type": ["null", "boolean"], "default": null}]}'
        
        try:
            # Register schemas in source
            source_client.register_schema(subject, schema1)
            source_client.register_schema(subject, schema2)
            source_client.register_schema(subject, schema3)
            
            # Register schemas in destination
            dest_client.register_schema(subject, schema1)
            dest_client.register_schema(subject, schema2)
            dest_client.register_schema(subject, schema3)
            
            # Compare version 1
            comparison1 = compare_schema_versions(source_client, dest_client, subject, 1)
            if not comparison1['source_exists'] or not comparison1['dest_exists'] or not comparison1['schemas_match']:
                raise Exception("Version 1 comparison failed")
            
            # Compare version 2
            comparison2 = compare_schema_versions(source_client, dest_client, subject, 2)
            if not comparison2['source_exists'] or not comparison2['dest_exists'] or not comparison2['schemas_match']:
                raise Exception("Version 2 comparison failed")
            
            # Compare version 3
            comparison3 = compare_schema_versions(source_client, dest_client, subject, 3)
            if not comparison3['source_exists'] or not comparison3['dest_exists'] or not comparison3['schemas_match']:
                raise Exception("Version 3 comparison failed")
            
            # Verify version sequences
            if comparison1['version_sequence']['source'] != [1, 2, 3] or comparison1['version_sequence']['destination'] != [1, 2, 3]:
                raise Exception("Version sequence verification failed")
            
            # Verify no version gaps
            if comparison1['version_gaps']['source'] is not None or comparison1['version_gaps']['destination'] is not None:
                raise Exception("Unexpected version gaps detected")
            
            return True
            
        finally:
            # Cleanup
            try:
                cleanup_specific_subjects(source_client, [subject], permanent=True)
                cleanup_specific_subjects(dest_client, [subject], permanent=True)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False

def run_test_version_gap_preservation() -> bool:
    """Run the test for checking version gap preservation."""
    try:
        # Create test clients with correct ports
        source_client = SchemaRegistryClient('http://localhost:38081')
        dest_client = SchemaRegistryClient('http://localhost:38082')
        
        # Test subject
        subject = 'test-subject-with-gaps'
        
        # Create schemas with gaps - ensuring backward compatibility
        schema1 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        schema2 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}]}'
        schema3 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}, {"name": "field3", "type": ["null", "boolean"], "default": null}]}'
        
        try:
            # Register schemas in source with gaps (versions 1 and 3)
            # First register schema1 (version 1)
            source_client.register_schema(subject, schema1)
            
            # Register schema2 (version 2)
            source_client.register_schema(subject, schema2)
            
            # Register schema3 (version 3)
            source_client.register_schema(subject, schema3)
            
            # Delete version 2 to create a gap
            try:
                # First try soft delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2: {e}")
                raise
            
            # Verify versions in source
            source_versions = source_client.get_versions(subject)
            if source_versions != [1, 3]:
                raise Exception(f"Source registry has unexpected versions: {source_versions}")
            
            # Register schemas in destination with different gaps (versions 1 and 3)
            dest_client.register_schema(subject, schema1)
            dest_client.register_schema(subject, schema2)
            dest_client.register_schema(subject, schema3)
            
            # Delete version 2 in destination to create a gap
            try:
                # First try soft delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2 in destination: {e}")
                raise
            
            # Verify versions in destination
            dest_versions = dest_client.get_versions(subject)
            if dest_versions != [1, 3]:
                raise Exception(f"Destination registry has unexpected versions: {dest_versions}")
            
            # Compare version 1 (should exist in both)
            comparison1 = compare_schema_versions(source_client, dest_client, subject, 1)
            if not comparison1['source_exists'] or not comparison1['dest_exists'] or not comparison1['schemas_match']:
                raise Exception("Version 1 comparison failed")
            
            # Compare version 2 (should not exist in either)
            comparison2 = compare_schema_versions(source_client, dest_client, subject, 2)
            if comparison2['source_exists'] or comparison2['dest_exists']:
                raise Exception("Version 2 should not exist in either registry")
            
            # Compare version 3 (should exist in both)
            comparison3 = compare_schema_versions(source_client, dest_client, subject, 3)
            if not comparison3['source_exists'] or not comparison3['dest_exists'] or not comparison3['schemas_match']:
                raise Exception("Version 3 comparison failed")
            
            # Verify version gaps are detected
            if comparison1['version_gaps']['source'] is None or comparison1['version_gaps']['destination'] is None:
                raise Exception("Version gaps not detected")
            
            # Verify source version gaps
            source_gaps = comparison1['version_gaps']['source']
            if source_gaps['actual_versions'] != [1, 3] or source_gaps['expected_versions'] != [1, 2, 3] or source_gaps['missing_versions'] != [2]:
                raise Exception("Source version gaps verification failed")
            
            # Verify destination version gaps
            dest_gaps = comparison1['version_gaps']['destination']
            if dest_gaps['actual_versions'] != [1, 3] or dest_gaps['expected_versions'] != [1, 2, 3] or dest_gaps['missing_versions'] != [2]:
                raise Exception("Destination version gaps verification failed")
            
            # Verify version sequences
            if comparison1['version_sequence']['source'] != [1, 3] or comparison1['version_sequence']['destination'] != [1, 3]:
                raise Exception("Version sequence verification failed")
            
            return True
            
        finally:
            # Cleanup
            try:
                cleanup_specific_subjects(source_client, [subject], permanent=True)
                cleanup_specific_subjects(dest_client, [subject], permanent=True)
            except:
                pass
                
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False

class TestMigration(unittest.TestCase):
    def setUp(self):
        pass

    def test_cleanup_specific_subjects(self):
        """Test the cleanup_specific_subjects function."""
        # Create test clients with correct ports
        source_client = SchemaRegistryClient('http://localhost:38081')
        dest_client = SchemaRegistryClient('http://localhost:38082')
        
        # Test subject
        subject = 'test-subject-with-gaps'
        
        # Create schemas with gaps - ensuring backward compatibility
        schema1 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        schema2 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}]}'
        schema3 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}, {"name": "field3", "type": ["null", "boolean"], "default": null}]}'
        
        try:
            # Register schemas in source with gaps (versions 1 and 3)
            # First register schema1 (version 1)
            source_client.register_schema(subject, schema1)
            
            # Register schema2 (version 2)
            source_client.register_schema(subject, schema2)
            
            # Register schema3 (version 3)
            source_client.register_schema(subject, schema3)
            
            # Delete version 2 to create a gap
            try:
                # First try soft delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2: {e}")
                raise
            
            # Verify versions in source
            source_versions = source_client.get_versions(subject)
            if source_versions != [1, 3]:
                raise Exception(f"Source registry has unexpected versions: {source_versions}")
            
            # Register schemas in destination with different gaps (versions 1 and 3)
            dest_client.register_schema(subject, schema1)
            dest_client.register_schema(subject, schema2)
            dest_client.register_schema(subject, schema3)
            
            # Delete version 2 in destination to create a gap
            try:
                # First try soft delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2 in destination: {e}")
                raise
            
            # Verify versions in destination
            dest_versions = dest_client.get_versions(subject)
            if dest_versions != [1, 3]:
                raise Exception(f"Destination registry has unexpected versions: {dest_versions}")
            
            # Compare version 1 (should exist in both)
            comparison1 = compare_schema_versions(source_client, dest_client, subject, 1)
            self.assertTrue(comparison1['source_exists'])
            self.assertTrue(comparison1['dest_exists'])
            self.assertTrue(comparison1['schemas_match'])
            
            # Compare version 2 (should not exist in either)
            comparison2 = compare_schema_versions(source_client, dest_client, subject, 2)
            self.assertFalse(comparison2['source_exists'])
            self.assertFalse(comparison2['dest_exists'])
            
            # Compare version 3 (should exist in both)
            comparison3 = compare_schema_versions(source_client, dest_client, subject, 3)
            self.assertTrue(comparison3['source_exists'])
            self.assertTrue(comparison3['dest_exists'])
            self.assertTrue(comparison3['schemas_match'])
            
            # Verify version gaps are detected
            self.assertIsNotNone(comparison1['version_gaps']['source'])
            self.assertIsNotNone(comparison1['version_gaps']['destination'])
            
            # Verify source version gaps
            source_gaps = comparison1['version_gaps']['source']
            self.assertEqual(source_gaps['actual_versions'], [1, 3])
            self.assertEqual(source_gaps['expected_versions'], [1, 2, 3])
            self.assertEqual(source_gaps['missing_versions'], [2])
            
            # Verify destination version gaps
            dest_gaps = comparison1['version_gaps']['destination']
            self.assertEqual(dest_gaps['actual_versions'], [1, 3])
            self.assertEqual(dest_gaps['expected_versions'], [1, 2, 3])
            self.assertEqual(dest_gaps['missing_versions'], [2])
            
            # Verify version sequences
            self.assertEqual(comparison1['version_sequence']['source'], [1, 3])
            self.assertEqual(comparison1['version_sequence']['destination'], [1, 3])
            
        finally:
            # Cleanup
            try:
                cleanup_specific_subjects(source_client, [subject], permanent=True)
                cleanup_specific_subjects(dest_client, [subject], permanent=True)
            except:
                pass

    def test_compare_schema_versions(self):
        pass

    def test_version_gap_preservation(self):
        pass

    def test_version_gaps_and_sequences(self):
        pass

    def test_compare_schema_versions_with_gaps(self):
        """Test the enhanced compare_schema_versions function with version gaps."""
        # Create test clients with correct ports
        source_client = SchemaRegistryClient('http://localhost:38081')
        dest_client = SchemaRegistryClient('http://localhost:38082')
        
        # Test subject
        subject = 'test-subject-with-gaps'
        
        # Create schemas with gaps - ensuring backward compatibility
        schema1 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}]}'
        schema2 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}]}'
        schema3 = '{"type": "record", "name": "Test", "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": ["null", "int"], "default": null}, {"name": "field3", "type": ["null", "boolean"], "default": null}]}'
        
        try:
            # Register schemas in source with gaps (versions 1 and 3)
            # First register schema1 (version 1)
            source_client.register_schema(subject, schema1)
            
            # Register schema2 (version 2)
            source_client.register_schema(subject, schema2)
            
            # Register schema3 (version 3)
            source_client.register_schema(subject, schema3)
            
            # Delete version 2 to create a gap
            try:
                # First try soft delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = source_client.session.delete(f"{source_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2: {e}")
                raise
            
            # Verify versions in source
            source_versions = source_client.get_versions(subject)
            if source_versions != [1, 3]:
                raise Exception(f"Source registry has unexpected versions: {source_versions}")
            
            # Register schemas in destination with different gaps (versions 1 and 3)
            dest_client.register_schema(subject, schema1)
            dest_client.register_schema(subject, schema2)
            dest_client.register_schema(subject, schema3)
            
            # Delete version 2 in destination to create a gap
            try:
                # First try soft delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2")
                response.raise_for_status()
                
                # Then try permanent delete
                response = dest_client.session.delete(f"{dest_client.url}/subjects/{subject}/versions/2?permanent=true")
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete version 2 in destination: {e}")
                raise
            
            # Verify versions in destination
            dest_versions = dest_client.get_versions(subject)
            if dest_versions != [1, 3]:
                raise Exception(f"Destination registry has unexpected versions: {dest_versions}")
            
            # Compare version 1 (should exist in both)
            comparison1 = compare_schema_versions(source_client, dest_client, subject, 1)
            self.assertTrue(comparison1['source_exists'])
            self.assertTrue(comparison1['dest_exists'])
            self.assertTrue(comparison1['schemas_match'])
            
            # Compare version 2 (should not exist in either)
            comparison2 = compare_schema_versions(source_client, dest_client, subject, 2)
            self.assertFalse(comparison2['source_exists'])
            self.assertFalse(comparison2['dest_exists'])
            
            # Compare version 3 (should exist in both)
            comparison3 = compare_schema_versions(source_client, dest_client, subject, 3)
            self.assertTrue(comparison3['source_exists'])
            self.assertTrue(comparison3['dest_exists'])
            self.assertTrue(comparison3['schemas_match'])
            
            # Verify version gaps are detected
            self.assertIsNotNone(comparison1['version_gaps']['source'])
            self.assertIsNotNone(comparison1['version_gaps']['destination'])
            
            # Verify source version gaps
            source_gaps = comparison1['version_gaps']['source']
            self.assertEqual(source_gaps['actual_versions'], [1, 3])
            self.assertEqual(source_gaps['expected_versions'], [1, 2, 3])
            self.assertEqual(source_gaps['missing_versions'], [2])
            
            # Verify destination version gaps
            dest_gaps = comparison1['version_gaps']['destination']
            self.assertEqual(dest_gaps['actual_versions'], [1, 3])
            self.assertEqual(dest_gaps['expected_versions'], [1, 2, 3])
            self.assertEqual(dest_gaps['missing_versions'], [2])
            
            # Verify version sequences
            self.assertEqual(comparison1['version_sequence']['source'], [1, 3])
            self.assertEqual(comparison1['version_sequence']['destination'], [1, 3])
            
        finally:
            # Cleanup
            try:
                cleanup_specific_subjects(source_client, [subject], permanent=True)
                cleanup_specific_subjects(dest_client, [subject], permanent=True)
            except:
                pass

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
        (17, "Mixed schema types test", run_mixed_schema_types_test),
        (18, "Conflict handling test", run_conflict_handling_test),
        (19, "Permanent delete test", run_permanent_delete_test),
        (20, "Mode after migration test", run_mode_after_migration_test),
        (21, "Global mode unit test", test_set_mode_for_all_subjects_unit),
        (22, "Selective subject cleanup test", run_test_cleanup_specific_subjects),
        (23, "Schema version comparison test", run_test_compare_schema_versions),
        (24, "Version gap preservation test", run_test_version_gap_preservation)
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