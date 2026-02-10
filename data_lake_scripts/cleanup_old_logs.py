"""
Log Cleanup Script
Searches all logs/ folders in the project and keeps only the last 2 log files for each log type.
"""

import os
import glob
from pathlib import Path
from collections import defaultdict
import re

def get_log_base_name(log_file):
    """
    Extract the base name of a log file (without timestamp).
    Example: '01_source_to_bronze_20260210_110640.log' -> '01_source_to_bronze'
    """
    filename = os.path.basename(log_file)
    # Remove timestamp pattern (YYYYMMDD_HHMMSS) and extension
    base_name = re.sub(r'_\d{8}_\d{6}\.log$', '', filename)
    return base_name

def cleanup_logs(project_root):
    """
    Find all logs/ folders and keep only the last 2 log files for each log type.
    """
    print("=" * 60)
    print("LOG CLEANUP SCRIPT")
    print("=" * 60)
    print()
    
    # Find all logs directories
    log_dirs = []
    for root, dirs, files in os.walk(project_root):
        if 'logs' in dirs:
            log_path = os.path.join(root, 'logs')
            log_dirs.append(log_path)
    
    print(f"Found {len(log_dirs)} log directories:")
    for log_dir in log_dirs:
        rel_path = os.path.relpath(log_dir, project_root)
        print(f"  - {rel_path}")
    print()
    
    total_deleted = 0
    total_kept = 0
    
    for log_dir in log_dirs:
        print(f"Processing: {os.path.relpath(log_dir, project_root)}")
        
        # Find all .log files
        log_files = glob.glob(os.path.join(log_dir, '*.log'))
        
        if not log_files:
            print("  No log files found")
            print()
            continue
        
        # Group log files by base name
        log_groups = defaultdict(list)
        for log_file in log_files:
            base_name = get_log_base_name(log_file)
            log_groups[base_name].append(log_file)
        
        # Process each group
        for base_name, files in log_groups.items():
            # Sort by modification time (newest first)
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            # Keep the first 2, delete the rest
            to_keep = files[:2]
            to_delete = files[2:]
            
            if to_delete:
                print(f"  {base_name}: Keeping {len(to_keep)}, deleting {len(to_delete)}")
                for file_to_delete in to_delete:
                    try:
                        os.remove(file_to_delete)
                        total_deleted += 1
                    except Exception as e:
                        print(f"    ERROR deleting {os.path.basename(file_to_delete)}: {e}")
            
            total_kept += len(to_keep)
        
        print()
    
    print("=" * 60)
    print("CLEANUP SUMMARY")
    print("=" * 60)
    print(f"Total log files kept: {total_kept}")
    print(f"Total log files deleted: {total_deleted}")
    print()

if __name__ == "__main__":
    # Get project root (parent of data_lake_scripts)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    print(f"Project root: {project_root}")
    print()
    
    cleanup_logs(project_root)
    
    print("Log cleanup completed successfully!")
