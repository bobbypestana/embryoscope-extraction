#!/usr/bin/env python3
import sys
import json
import re
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REGISTRY_FILE = os.path.join(SCRIPT_DIR, "project_pids.json")

def main():
    try:
        raw_stdin = sys.stdin.read()
        if not raw_stdin.strip():
            sys.exit(0)
            
        data = json.loads(raw_stdin)
        metadata = data.get("metadata", {})
        agent_name = metadata.get("agent_name") or metadata.get("agent_id") or "unknown_agent"
        
        tool_response = str(data.get("tool_response", "")) + " " + str(data.get("tool_output", ""))
        tool_input = str(data.get("tool_input", ""))
        
        # Match PID outputs from command execution (e.g. Process ID: 12345, PID 12345, TaskId: ...)
        found_pids = set()
        for match in re.finditer(r"(?:PID|Process ID|ProcessId|id)\s*[:=]?\s*(\d+)", tool_response, re.IGNORECASE):
            found_pids.add(match.group(1))
            
        for match in re.finditer(r"(?:PID|Process ID|ProcessId|id)\s*[:=]?\s*(\d+)", tool_input, re.IGNORECASE):
            found_pids.add(match.group(1))
            
        if not found_pids:
            sys.exit(0)

        registry = {}
        if os.path.exists(REGISTRY_FILE):
            try:
                with open(REGISTRY_FILE, "r") as f:
                    registry = json.load(f)
            except Exception:
                registry = {}
                
        for pid in found_pids:
            registry[str(pid)] = agent_name

        with open(REGISTRY_FILE, "w") as f:
            json.dump(registry, f, indent=2)
            
    except Exception:
        sys.exit(0)

if __name__ == "__main__":
    main()
