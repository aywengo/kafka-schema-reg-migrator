#!/usr/bin/env python3

import os
import json
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

def register_schema(session: requests.Session, url: str, subject: str, schema: dict) -> None:
    """Register a schema in the registry."""
    try:
        response = session.post(
            f"{url}/subjects/{subject}/versions",
            json={"schema": json.dumps(schema)}
        )
        response.raise_for_status()
        logger.info(f"Successfully registered schema for subject {subject}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to register schema for subject {subject}: {e}")
        raise

def main():
    source_url = 'http://localhost:38081'
    session = create_session_with_retries()
    
    # Test schema 1
    schema1 = {
        "type": "record",
        "name": "TestValue",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    }
    
    # Test schema 2
    schema2 = {
        "type": "record",
        "name": "TestValueV2",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}
        ]
    }
    
    # Test schema 3
    schema3 = {
        "type": "record",
        "name": "TestValueV3",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }
    
    try:
        # Register schemas
        register_schema(session, source_url, "test-value", schema1)
        register_schema(session, source_url, "test-value-v2", schema2)
        register_schema(session, source_url, "test-value-v3", schema3)
        
        logger.info("Successfully populated source registry with test schemas")
    except Exception as e:
        logger.error(f"Failed to populate source registry: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 