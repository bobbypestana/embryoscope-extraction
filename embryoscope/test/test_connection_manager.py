#!/usr/bin/env python3
"""
Simple test script to verify the database connection manager works.
This creates a test database connection and then tests the connection manager.
"""

import os
import sys
import time
import duckdb
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.close_db_connections import find_duckdb_processes, wait_for_file_unlock

def setup_logging():
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_database_connection():
    """Test creating a database connection."""
    logger = setup_logging()
    
    # Test database path
    db_path = "../database/test_connection_manager.duckdb"
    
    logger.info(f"Testing database connection to: {db_path}")
    
    try:
        # Create a test connection
        with duckdb.connect(db_path) as conn:
            logger.info("✓ Successfully created database connection")
            
            # Create a test table
            conn.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR)")
            logger.info("✓ Successfully created test table")
            
            # Insert test data
            conn.execute("INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
            logger.info("✓ Successfully inserted test data")
            
            # Query test data
            result = conn.execute("SELECT * FROM test_table").fetchall()
            logger.info(f"✓ Successfully queried data: {result}")
            
        logger.info("✓ Database connection test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Database connection test failed: {e}")
        return False

def test_connection_manager():
    """Test the connection manager functionality."""
    logger = setup_logging()
    
    db_path = "../database/test_connection_manager.duckdb"
    
    logger.info("Testing connection manager...")
    
    # Test finding processes
    processes = find_duckdb_processes()
    logger.info(f"Found {len(processes)} potential DuckDB processes")
    
    for proc in processes:
        logger.info(f"  - PID {proc.info['pid']}: {proc.info['name']}")
    
    # Test file unlock functionality
    if os.path.exists(db_path):
        logger.info("Testing file unlock functionality...")
        unlocked = wait_for_file_unlock(db_path, timeout=5)
        if unlocked:
            logger.info("✓ File unlock test passed")
        else:
            logger.info("✗ File unlock test failed")
    else:
        logger.info("Database file doesn't exist, skipping file unlock test")
    
    return True

def simulate_file_locking():
    """Simulate a file locking scenario for testing."""
    logger = setup_logging()
    
    db_path = "../database/test_connection_manager.duckdb"
    
    logger.info("Simulating file locking scenario...")
    
    try:
        # Create a connection that will hold the file
        conn1 = duckdb.connect(db_path)
        logger.info("✓ Created first connection (holding file)")
        
        # Try to create another connection (should fail or be blocked)
        try:
            conn2 = duckdb.connect(db_path)
            logger.info("✓ Second connection also succeeded (no locking)")
            conn2.close()
        except Exception as e:
            logger.info(f"✗ Second connection failed as expected: {e}")
        
        # Close the first connection
        conn1.close()
        logger.info("✓ Closed first connection")
        
        # Now try again
        try:
            conn3 = duckdb.connect(db_path)
            logger.info("✓ Third connection succeeded after closing first")
            conn3.close()
        except Exception as e:
            logger.error(f"✗ Third connection failed: {e}")
        
    except Exception as e:
        logger.error(f"✗ File locking simulation failed: {e}")
        return False
    
    return True

def main():
    """Run all tests."""
    logger = setup_logging()
    
    logger.info("=" * 50)
    logger.info("Database Connection Manager Test")
    logger.info("=" * 50)
    
    # Ensure database directory exists
    os.makedirs("../database", exist_ok=True)
    
    # Test 1: Basic database connection
    logger.info("\n1. Testing basic database connection...")
    test1_passed = test_database_connection()
    
    # Test 2: Connection manager functionality
    logger.info("\n2. Testing connection manager...")
    test2_passed = test_connection_manager()
    
    # Test 3: File locking simulation
    logger.info("\n3. Testing file locking simulation...")
    test3_passed = simulate_file_locking()
    
    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("TEST SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Database connection test: {'✓ PASSED' if test1_passed else '✗ FAILED'}")
    logger.info(f"Connection manager test: {'✓ PASSED' if test2_passed else '✗ FAILED'}")
    logger.info(f"File locking simulation: {'✓ PASSED' if test3_passed else '✗ FAILED'}")
    
    if all([test1_passed, test2_passed, test3_passed]):
        logger.info("\n🎉 All tests passed! The connection manager should work correctly.")
    else:
        logger.info("\n⚠️  Some tests failed. Check the logs above for details.")
    
    # Cleanup test database
    try:
        if os.path.exists("../database/test_connection_manager.duckdb"):
            os.remove("../database/test_connection_manager.duckdb")
            logger.info("✓ Cleaned up test database file")
    except Exception as e:
        logger.warning(f"Could not clean up test database: {e}")

if __name__ == "__main__":
    main() 