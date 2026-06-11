#!/usr/bin/env python3
import sys
import json
import re
import os

REGISTRY_FILE = ".antigravity/project_pids.json"

def main():
    try:
        input_data = json.loads(sys.stdin.read())
        tool_input = input_data.get("tool_input", {})
        command_line = tool_input.get("content", "") or tool_input.get("CommandLine", "")
        
        # Identify which agent is making the request (Antigravity passes metadata)
        agent_name = input_data.get("metadata", {}).get("agent_name", "unknown_agent")
    except Exception:
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)

    # Catch specific PID kill attempts
    match = re.search(r"/PID\s+(\d+)|-Id\s+(\d+)", command_line, re.IGNORECASE)
    if match:
        target_pid = match.group(1) or match.group(2)
        
        # Check the central registry
        if os.path.exists(REGISTRY_FILE):
            with open(REGISTRY_FILE, "r") as f:
                registry = json.load(f)
            
            # If the PID is registered to someone else, block it!
            if target_pid in registry and registry[target_pid] != agent_name:
                print(json.dumps({
                    "decision": "deny",
                    "reason": f"Access Denied. PID {target_pid} belongs to {registry[target_pid]}. You cannot terminate it.",
                    "systemMessage": f"🚨 Security Block: You tried to kill a process owned by {registry[target_pid]}."
                }))
                sys.exit(0)

    print(json.dumps({"decision": "allow"}))

if __name__ == "__main__":
    main()