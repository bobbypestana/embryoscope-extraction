"""
Cleanup Script for Image Availability Pipeline
Keeps only the last 3 log files and last 3 API result directories.
Helps prevent disk space bloat from accumulated logs and JSON files.
"""

import os
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cleanup_logs(log_dir: str, keep_count: int = 3):
    """
    Keep only the most recent N log files.
    
    Args:
        log_dir: Directory containing log files
        keep_count: Number of recent logs to keep
    """
    log_path = Path(log_dir)
    
    if not log_path.exists():
        logger.warning(f"Log directory does not exist: {log_dir}")
        return
    
    # Get all log files (excluding server-specific logs)
    log_files = []
    for log_file in log_path.glob('*.log'):
        # Skip server-specific logs (they contain server names)
        if any(server in log_file.name for server in ['Ibirapuera', 'Belo Horizonte', 'Paulista', 'Morumbi']):
            continue
        log_files.append(log_file)
    
    if len(log_files) <= keep_count:
        logger.info(f"Found {len(log_files)} log files (keeping all, threshold is {keep_count})")
        return
    
    # Sort by modification time (newest first)
    log_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    
    # Keep the most recent N, delete the rest
    files_to_keep = log_files[:keep_count]
    files_to_delete = log_files[keep_count:]
    
    logger.info(f"Found {len(log_files)} log files")
    logger.info(f"Keeping {len(files_to_keep)} most recent logs")
    logger.info(f"Deleting {len(files_to_delete)} old logs")
    
    for log_file in files_to_delete:
        try:
            log_file.unlink()
            logger.info(f"  Deleted: {log_file.name}")
        except Exception as e:
            logger.error(f"  Failed to delete {log_file.name}: {e}")


def cleanup_api_results(results_dir: str, keep_count: int = 3):
    """
    Keep only the most recent N API result directories.
    
    Args:
        results_dir: Parent directory containing API result subdirectories
        keep_count: Number of recent result directories to keep
    """
    results_path = Path(results_dir)
    
    if not results_path.exists():
        logger.warning(f"API results directory does not exist: {results_dir}")
        return
    
    # Get all subdirectories
    result_dirs = [d for d in results_path.iterdir() if d.is_dir()]
    
    if len(result_dirs) <= keep_count:
        logger.info(f"Found {len(result_dirs)} result directories (keeping all, threshold is {keep_count})")
        return
    
    # Sort by modification time (newest first)
    result_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    
    # Keep the most recent N, delete the rest
    dirs_to_keep = result_dirs[:keep_count]
    dirs_to_delete = result_dirs[keep_count:]
    
    logger.info(f"Found {len(result_dirs)} API result directories")
    logger.info(f"Keeping {len(dirs_to_keep)} most recent directories")
    logger.info(f"Deleting {len(dirs_to_delete)} old directories")
    
    for result_dir in dirs_to_delete:
        try:
            # Delete all files in the directory
            for file in result_dir.glob('*'):
                file.unlink()
            # Delete the directory itself
            result_dir.rmdir()
            logger.info(f"  Deleted: {result_dir.name}")
        except Exception as e:
            logger.error(f"  Failed to delete {result_dir.name}: {e}")


def main():
    logger.info("="*80)
    logger.info("CLEANUP: Image Availability Pipeline")
    logger.info("="*80)
    
    # Cleanup logs
    script_dir = Path(__file__).parent
    logger.info("\nCleaning up log files...")
    cleanup_logs(str(script_dir / 'logs'), keep_count=3)
    
    # Cleanup API results
    logger.info("\nCleaning up API result directories...")
    cleanup_api_results(str(script_dir / 'api_results'), keep_count=3)
    
    logger.info("\n" + "="*80)
    logger.info("CLEANUP COMPLETE!")
    logger.info("="*80)


if __name__ == "__main__":
    main()
