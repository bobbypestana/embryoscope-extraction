#!/usr/bin/env python3
"""
Utility script to close any lingering DuckDB connections.
Run this if you encounter "file is being used by another process" errors.
"""

import os
import sys
import time
import psutil
import logging

def find_duckdb_processes():
    """Find processes that might be holding DuckDB connections."""
    duckdb_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
            if 'duckdb' in cmdline.lower() or 'python' in proc.info['name'].lower():
                # Check if it's our embryoscope extraction process
                if 'embryoscope' in cmdline.lower() or 'try_request' in cmdline.lower():
                    duckdb_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return duckdb_processes

def close_duckdb_connections():
    """Attempt to close DuckDB connections gracefully."""
    processes = find_duckdb_processes()
    
    if not processes:
        print("No DuckDB-related processes found.")
        return
    
    print(f"Found {len(processes)} potential DuckDB processes:")
    for proc in processes:
        print(f"  PID {proc.info['pid']}: {proc.info['name']}")
        print(f"    Command: {' '.join(proc.info['cmdline'])}")
    
    response = input("\nDo you want to terminate these processes? (y/N): ")
    if response.lower() != 'y':
        print("No processes terminated.")
        return
    
    for proc in processes:
        try:
            print(f"Terminating PID {proc.info['pid']}...")
            proc.terminate()
            proc.wait(timeout=5)
            print(f"Successfully terminated PID {proc.info['pid']}")
        except psutil.TimeoutExpired:
            print(f"Force killing PID {proc.info['pid']}...")
            proc.kill()
        except psutil.NoSuchProcess:
            print(f"Process PID {proc.info['pid']} already terminated")
        except Exception as e:
            print(f"Error terminating PID {proc.info['pid']}: {e}")

def wait_for_file_unlock(db_path, timeout=30):
    """Wait for database file to be unlocked."""
    print(f"Waiting for database file to be unlocked: {db_path}")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Try to open the file in write mode
            with open(db_path, 'r+b'):
                print("Database file is now unlocked!")
                return True
        except PermissionError:
            print(f"File still locked, waiting... ({time.time() - start_time:.1f}s)")
            time.sleep(1)
        except FileNotFoundError:
            print("Database file doesn't exist yet.")
            return True
    
    print(f"Timeout waiting for file unlock after {timeout} seconds")
    return False

if __name__ == "__main__":
    # Database path from params.yml
    db_path = "../database/embryoscope_vila_mariana.db"
    
    print("DuckDB Connection Manager")
    print("=" * 40)
    
    # Check if database file exists
    if os.path.exists(db_path):
        print(f"Database file found: {db_path}")
        
        # Check if file is locked
        try:
            with open(db_path, 'r+b'):
                print("Database file is not locked.")
        except PermissionError:
            print("Database file is locked by another process.")
            close_duckdb_connections()
            wait_for_file_unlock(db_path)
    else:
        print(f"Database file not found: {db_path}")
        print("This is normal for first-time runs.")
    
    print("\nDone!") 