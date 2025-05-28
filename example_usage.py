#!/usr/bin/env python3
"""
Example usage of the Schema Registry Migrator with mode handling and ID preservation.

This script demonstrates how to:
1. Handle subjects in read-only mode
2. Preserve original schema IDs
3. Retry failed migrations
"""

import os
import sys
from schema_registry_migrator import SchemaRegistryClient, migrate_schemas, retry_failed_migrations

def example_migration_with_mode_handling():
    """Example of migrating schemas with automatic mode handling."""
    
    # Initialize clients
    source_client = SchemaRegistryClient(
        url="http://source-registry:8081",
        username="source_user",
        password="source_password"
    )
    
    dest_client = SchemaRegistryClient(
        url="http://dest-registry:8082",
        username="dest_user",
        password="dest_password",
        import_mode=True  # Enable import mode for ID preservation
    )
    
    # Perform migration with ID preservation
    print("Starting migration with ID preservation...")
    results = migrate_schemas(
        source_client, 
        dest_client, 
        dry_run=False,
        preserve_ids=True
    )
    
    # Check for failed migrations
    if results['failed']:
        print(f"\n{len(results['failed'])} migrations failed. Retrying with mode changes...")
        
        # Retry failed migrations
        retry_results = retry_failed_migrations(
            source_client,
            dest_client,
            results['failed'],
            preserve_ids=True
        )
        
        print(f"Retry complete: {len(retry_results['successful'])} succeeded, {len(retry_results['failed'])} failed")
    
    return results

def example_check_and_fix_subject_modes():
    """Example of checking and fixing subject modes before migration."""
    
    client = SchemaRegistryClient(
        url="http://dest-registry:8082",
        username="dest_user",
        password="dest_password"
    )
    
    # Get all subjects
    subjects = client.get_subjects()
    
    # Check and fix modes
    for subject in subjects:
        mode = client.get_subject_mode(subject)
        if mode != 'READWRITE':
            print(f"Subject {subject} is in {mode} mode, changing to READWRITE")
            client.set_subject_mode(subject, 'READWRITE')
            print(f"Subject {subject} mode changed to READWRITE")

if __name__ == "__main__":
    # Example 1: Migration with automatic mode handling
    print("Example 1: Migration with automatic mode handling")
    print("-" * 50)
    example_migration_with_mode_handling()
    
    # Example 2: Check and fix subject modes
    print("\n\nExample 2: Check and fix subject modes")
    print("-" * 50)
    example_check_and_fix_subject_modes() 