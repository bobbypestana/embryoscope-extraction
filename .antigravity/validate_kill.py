#!/usr/bin/env python3
import sys
import json
import re
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REGISTRY_FILE = os.path.join(SCRIPT_DIR, "project_pids.json")

def extract_command_line(input_data):
    """Extract command line string from various possible tool_input structures."""
    tool_input = input_data.get("tool_input", {})
    if isinstance(tool_input, str):
        return tool_input
    
    # Check common fields in tool_input
    for key in ["CommandLine", "content", "command", "cmd", "args", "TaskId"]:
        val = tool_input.get(key)
        if val:
            if isinstance(val, list):
                return " ".join(str(v) for v in val)
            return str(val)
    return str(tool_input)

def main():
    try:
        raw_stdin = sys.stdin.read()
        if not raw_stdin.strip():
            print(json.dumps({"decision": "allow"}))
            sys.exit(0)
            
        input_data = json.loads(raw_stdin)
        command_line = extract_command_line(input_data)
        
        # Identify which agent is making the request
        metadata = input_data.get("metadata", {})
        agent_name = metadata.get("agent_name") or metadata.get("agent_id") or "unknown_agent"
    except Exception as e:
        # If parsing failed but termination keywords exist in raw text, force inspection
        raw_stdin_str = locals().get("raw_stdin", "")
        lowered = raw_stdin_str.lower()
        if any(kw in lowered for kw in ["taskkill", "stop-process", "wmic", "kill"]):
            command_line = raw_stdin_str
            agent_name = "unknown_agent"
        else:
            print(json.dumps({"decision": "allow"}))
            sys.exit(0)

    # Patterns to match process termination and PIDs:
    # 1. taskkill /PID 1234, taskkill /PID:1234, taskkill /pid 1234
    # 2. Stop-Process -Id 1234, Stop-Process 1234, kill 1234
    # 3. wmic process where processid=1234 delete
    # 4. -Id 1234, /PID 1234, /PID:1234
    patterns = [
        r"(?:taskkill|taskkill\.exe).*(?:/PID|/PID:)\s*:?\s*(\d+)",
        r"(?:Stop-Process|kill).*(?:-Id\s+|\s+)(\d+)",
        r"wmic\s+process.*processid\s*=\s*(\d+)",
        r"(?:/PID|/PID:)\s*:?\s*(\d+)",
        r"-Id\s*:?\s*(\d+)"
    ]

    target_pid = None
    for pattern in patterns:
        match = re.search(pattern, command_line, re.IGNORECASE)
        if match:
            target_pid = match.group(1)
            break

    # If general process kill command is detected without specific PID (e.g. taskkill /F /IM ..., Stop-Process -Name ...)
    is_generic_kill = bool(re.search(r"\b(?:taskkill|Stop-Process)\b", command_line, re.IGNORECASE))

    if target_pid or is_generic_kill:
        registry = {}
        if os.path.exists(REGISTRY_FILE):
            try:
                with open(REGISTRY_FILE, "r") as f:
                    registry = json.load(f)
            except Exception:
                registry = {}
        
        if target_pid:
            owner = registry.get(str(target_pid))
            if owner and owner != agent_name:
                print(json.dumps({
                    "decision": "deny",
                    "reason": f"Access Denied. PID {target_pid} belongs to agent '{owner}'. You cannot terminate it.",
                    "systemMessage": f"🚨 Security Block: Process termination denied. PID {target_pid} is owned by '{owner}'."
                }))
                sys.exit(0)
                
            if not owner:
                print(json.dumps({
                    "decision": "deny",
                    "reason": f"Access Denied. PID {target_pid} is not registered to your agent context ({agent_name}). Termination blocked by hook.",
                    "systemMessage": f"🚨 Security Block: Attempted to kill unregistered PID {target_pid}."
                }))
                sys.exit(0)
        elif is_generic_kill:
            print(json.dumps({
                "decision": "deny",
                "reason": f"Access Denied. Generic process termination command detected ('{command_line}'). Agents must not terminate external processes.",
                "systemMessage": f"🚨 Security Block: Generic process termination command blocked."
            }))
            sys.exit(0)

    print(json.dumps({"decision": "allow"}))

if __name__ == "__main__":
    main()
