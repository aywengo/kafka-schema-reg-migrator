#!/usr/bin/env python3
"""
Utility script to fix read-only subjects in Schema Registry.

This script can be used to:
1. List all subjects and their modes
2. Change specific subjects to READWRITE mode
3. Change all read-only subjects to READWRITE mode
"""

import os
import sys
import argparse
from dotenv import load_dotenv
from schema_registry_migrator import SchemaRegistryClient

# Load environment variables
load_dotenv()

def list_subject_modes(client: SchemaRegistryClient):
    """List all subjects and their current modes."""
    subjects = client.get_subjects()
    print(f"\nFound {len(subjects)} subjects:\n")
    
    readonly_count = 0
    for subject in subjects:
        mode = client.get_subject_mode(subject)
        status = "✓" if mode == "READWRITE" else "✗"
        print(f"{status} {subject}: {mode}")
        if mode != "READWRITE":
            readonly_count += 1
    
    print(f"\nSummary: {readonly_count} subjects are not in READWRITE mode")
    return readonly_count

def fix_subject_mode(client: SchemaRegistryClient, subject: str):
    """Fix a single subject's mode to READWRITE."""
    try:
        current_mode = client.get_subject_mode(subject)
        if current_mode == "READWRITE":
            print(f"Subject {subject} is already in READWRITE mode")
            return True
        
        print(f"Changing {subject} from {current_mode} to READWRITE...")
        client.set_subject_mode(subject, "READWRITE")
        print(f"✓ Successfully changed {subject} to READWRITE mode")
        return True
    except Exception as e:
        print(f"✗ Failed to change {subject} mode: {str(e)}")
        return False

def fix_all_readonly_subjects(client: SchemaRegistryClient):
    """Fix all read-only subjects to READWRITE mode."""
    subjects = client.get_subjects()
    fixed_count = 0
    failed_count = 0
    
    for subject in subjects:
        mode = client.get_subject_mode(subject)
        if mode != "READWRITE":
            if fix_subject_mode(client, subject):
                fixed_count += 1
            else:
                failed_count += 1
    
    print(f"\nFixed {fixed_count} subjects, {failed_count} failed")

def main():
    parser = argparse.ArgumentParser(description="Fix read-only subjects in Schema Registry")
    parser.add_argument("--list", action="store_true", help="List all subjects and their modes")
    parser.add_argument("--fix", metavar="SUBJECT", help="Fix a specific subject to READWRITE mode")
    parser.add_argument("--fix-all", action="store_true", help="Fix all read-only subjects to READWRITE mode")
    parser.add_argument("--url", help="Schema Registry URL (overrides DEST_SCHEMA_REGISTRY_URL)")
    parser.add_argument("--username", help="Username for authentication")
    parser.add_argument("--password", help="Password for authentication")
    
    args = parser.parse_args()
    
    # Initialize client
    url = args.url or os.getenv('DEST_SCHEMA_REGISTRY_URL', 'http://localhost:8082')
    username = args.username or os.getenv('DEST_USERNAME')
    password = args.password or os.getenv('DEST_PASSWORD')
    
    client = SchemaRegistryClient(
        url=url,
        username=username,
        password=password,
        context=os.getenv('DEST_CONTEXT')
    )
    
    print(f"Connected to Schema Registry: {url}")
    
    # Execute requested action
    if args.list:
        list_subject_modes(client)
    elif args.fix:
        fix_subject_mode(client, args.fix)
    elif args.fix_all:
        response = input("This will change ALL read-only subjects to READWRITE mode. Continue? (y/N): ")
        if response.lower() == 'y':
            fix_all_readonly_subjects(client)
        else:
            print("Operation cancelled")
    else:
        # Default action: list subjects
        list_subject_modes(client)
        print("\nUse --help to see available options")

if __name__ == "__main__":
    main() 