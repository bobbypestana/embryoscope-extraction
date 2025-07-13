#!/usr/bin/env python3
"""
Launch extraction for all enabled clinics in parallel.
"""
import yaml
import subprocess
import sys
import os

PARAMS_PATH = '../params.yml'
EXTRACT_SCRIPT = '../embryoscope_extractor.py'

# Activate conda env if needed (Windows batch file should do this)

def main():
    # Read enabled clinics from params.yml
    with open(PARAMS_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    enabled = [k for k, v in config['embryoscope_credentials'].items() if v.get('enabled', False)]
    if not enabled:
        print("No enabled clinics found in params.yml.")
        sys.exit(1)
    print(f"Enabled clinics: {enabled}")
    print()
    procs = []
    for clinic in enabled:
        print(f"Launching extraction for: {clinic}")
        # Use sys.executable to ensure correct Python
        proc = subprocess.Popen([sys.executable, EXTRACT_SCRIPT, clinic])
        procs.append(proc)
    print("\nAll extractions launched in parallel. Each will open its own progress bar.")
    print("You can monitor the output in each terminal or process window.")
    print("(Press Ctrl+C here to exit this launcher, extractions will continue in background.)")
    # Optionally, wait for all to finish
    for proc in procs:
        proc.wait()

if __name__ == "__main__":
    main() 