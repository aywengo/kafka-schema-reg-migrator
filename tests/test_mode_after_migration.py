#!/usr/bin/env python3
"""
Pytest tests for DEST_MODE_AFTER_MIGRATION functionality.
"""

import os
import sys
import json
import time
import pytest
import requests
import subprocess
from typing import Dict, List

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from schema_registry_migrator import SchemaRegistryClient


@pytest.fixture
def session():
    """Create a requests session for tests."""
    return requests.Session()


@pytest.fixture
def test_schemas():
    """Test schemas for migration."""
    return [
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


def cleanup_registry(url: str, session: requests.Session):
    """Clean up a schema registry."""
    try:
        response = session.get(f"{url}/subjects")
        if response.status_code == 200:
            subjects = response.json()
            for subject in subjects:
                session.delete(f"{url}/subjects/{subject}?permanent=true")
    except:
        pass  # Ignore errors during cleanup


class TestModeAfterMigration:
    """Test class for DEST_MODE_AFTER_MIGRATION functionality."""
    
    def setup_method(self):
        """Setup before each test."""
        session = requests.Session()
        cleanup_registry("http://localhost:38081", session)
        cleanup_registry("http://localhost:38082", session)
        time.sleep(1)
    
    def test_global_mode_functions(self, session):
        """Test the global mode functions directly."""
        # Create test subjects
        subjects = ["test-func-1", "test-func-2", "test-func-3"]
        
        for subject in subjects:
            schema = {
                "type": "record",
                "name": f"Test{subject.replace('-', '')}",
                "fields": [{"name": "id", "type": "int"}]
            }
            response = session.post(
                f"http://localhost:38082/subjects/{subject}/versions",
                json={"schema": json.dumps(schema)}
            )
            assert response.status_code == 200
        
        # Create client and test global mode
        client = SchemaRegistryClient(url="http://localhost:38082")
        
        # Get initial mode
        initial_mode = client.get_global_mode()
        assert initial_mode in ['READWRITE', 'READONLY', 'READWRITE_OVERRIDE', 'IMPORT']
        
        # Set global mode to READONLY
        client.set_global_mode("READONLY")
        
        # Verify global mode is READONLY
        mode = client.get_global_mode()
        assert mode == "READONLY", f"Global mode should be READONLY, got {mode}"
        
        # Set back to READWRITE
        client.set_global_mode("READWRITE")
        
        # Verify global mode is READWRITE
        mode = client.get_global_mode()
        assert mode == "READWRITE", f"Global mode should be READWRITE, got {mode}"
    
    def test_mode_after_migration_readonly(self, session, test_schemas):
        """Test migration with DEST_MODE_AFTER_MIGRATION=READONLY."""
        # Create schemas in source
        for subject, schema in test_schemas:
            response = session.post(
                f"http://localhost:38081/subjects/{subject}/versions",
                json={"schema": json.dumps(schema)}
            )
            assert response.status_code == 200
        
        # Run migration with DEST_MODE_AFTER_MIGRATION=READONLY
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'DEST_MODE_AFTER_MIGRATION': 'READONLY'
        })
        
        result = subprocess.run(
            ['python', 'schema_registry_migrator.py'],
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Migration failed: {result.stderr}"
        
        # Verify global mode is READONLY
        response = session.get("http://localhost:38082/mode")
        assert response.status_code == 200
        mode_data = response.json()
        assert mode_data.get('mode') == 'READONLY', f"Global mode should be READONLY"
    
    def test_mode_after_migration_readwrite(self, session, test_schemas):
        """Test migration with DEST_MODE_AFTER_MIGRATION=READWRITE."""
        # Create schemas in source
        for subject, schema in test_schemas:
            response = session.post(
                f"http://localhost:38081/subjects/{subject}/versions",
                json={"schema": json.dumps(schema)}
            )
            assert response.status_code == 200
        
        # Run migration with DEST_MODE_AFTER_MIGRATION=READWRITE
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'DEST_MODE_AFTER_MIGRATION': 'READWRITE'
        })
        
        result = subprocess.run(
            ['python', 'schema_registry_migrator.py'],
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Migration failed: {result.stderr}"
        
        # Verify global mode is READWRITE
        client = SchemaRegistryClient(url="http://localhost:38082")
        mode = client.get_global_mode()
        assert mode == 'READWRITE', f"Global mode should be READWRITE"
    
    def test_mode_after_migration_with_import_mode(self, session, test_schemas):
        """Test DEST_MODE_AFTER_MIGRATION reverting from IMPORT mode."""
        # Create schemas in source
        for subject, schema in test_schemas:
            response = session.post(
                f"http://localhost:38081/subjects/{subject}/versions",
                json={"schema": json.dumps(schema)}
            )
            assert response.status_code == 200
        
        # Run migration with DEST_IMPORT_MODE=true and DEST_MODE_AFTER_MIGRATION=READWRITE
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'false',
            'DEST_IMPORT_MODE': 'true',
            'CLEANUP_DESTINATION': 'true',
            'DEST_MODE_AFTER_MIGRATION': 'READWRITE'
        })
        
        result = subprocess.run(
            ['python', 'schema_registry_migrator.py'],
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Migration failed: {result.stderr}"
        
        # Verify global mode is READWRITE (reverted from IMPORT)
        response = session.get("http://localhost:38082/mode")
        assert response.status_code == 200
        mode_data = response.json()
        mode = mode_data.get('mode')
        assert mode == 'READWRITE', f"Global mode should be READWRITE after import, got {mode}"
    
    def test_mode_after_migration_dry_run(self, session, test_schemas):
        """Test that DEST_MODE_AFTER_MIGRATION is not applied in dry-run mode."""
        # Create schemas in source
        for subject, schema in test_schemas:
            response = session.post(
                f"http://localhost:38081/subjects/{subject}/versions",
                json={"schema": json.dumps(schema)}
            )
            assert response.status_code == 200
        
        # Set initial global mode to READWRITE
        client = SchemaRegistryClient(url="http://localhost:38082")
        client.set_global_mode("READWRITE")
        
        # Run migration in DRY_RUN mode with DEST_MODE_AFTER_MIGRATION=READONLY
        env = os.environ.copy()
        env.update({
            'SOURCE_SCHEMA_REGISTRY_URL': 'http://localhost:38081',
            'DEST_SCHEMA_REGISTRY_URL': 'http://localhost:38082',
            'ENABLE_MIGRATION': 'true',
            'DRY_RUN': 'true',  # Dry run mode
            'DEST_MODE_AFTER_MIGRATION': 'READONLY'
        })
        
        result = subprocess.run(
            ['python', 'schema_registry_migrator.py'],
            env=env,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Dry run failed: {result.stderr}"
        
        # Verify the global mode was NOT changed (should still be READWRITE)
        mode = client.get_global_mode()
        assert mode == 'READWRITE', "Mode should not be changed in dry-run mode"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 