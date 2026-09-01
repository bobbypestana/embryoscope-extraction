#!/usr/bin/env python3
import sys
import json
import re

def extract_command_line(input_data):
    """Extract command line string or code arguments from tool_input or toolCall."""
    # 1. Check tool_input
    tool_input = input_data.get("tool_input", {})
    if isinstance(tool_input, str):
        return tool_input
    
    # 2. Check toolCall args
    tool_call = input_data.get("toolCall", {})
    tool_call_args = tool_call.get("args", {}) if isinstance(tool_call, dict) else {}
    
    # Combined search dictionary
    search_dicts = []
    if isinstance(tool_input, dict):
        search_dicts.append(tool_input)
    if isinstance(tool_call_args, dict):
        search_dicts.append(tool_call_args)
    
    # Check common fields
    for sd in search_dicts:
        for key in ["CommandLine", "content", "command", "cmd", "args", "CodeContent", "ReplacementContent"]:
            val = sd.get(key)
            if val:
                if isinstance(val, list):
                    return " ".join(str(v) for v in val)
                return str(val)
                
    return str(input_data)

def main():
    try:
        raw_stdin = sys.stdin.read()
        if not raw_stdin.strip():
            print(json.dumps({"decision": "allow"}))
            sys.exit(0)
            
        input_data = json.loads(raw_stdin)
        command_line = extract_command_line(input_data)
    except Exception:
        # If parsing fails but raw content contains drop table, safety first
        raw_stdin_str = locals().get("raw_stdin", "")
        if "drop table" in raw_stdin_str.lower():
            command_line = raw_stdin_str
        else:
            print(json.dumps({"decision": "allow"}))
            sys.exit(0)

    # Check for DROP TABLE SQL statements (with optional whitespace, quotes, or path qualifiers)
    # Pattern detects variations: DROP TABLE, DROP TABLE IF EXISTS, drop table `db`.`tbl`, etc.
    drop_table_pattern = r"\bdrop\s+table\s+(?:if\s+exists\s+)?[\w`\"'.\-]+"
    
    if re.search(drop_table_pattern, command_line, re.IGNORECASE):
        print(json.dumps({
            "decision": "ask",
            "reason": "DROP TABLE statement detected. Execution of drop table queries requires explicit confirmation.",
            "systemMessage": "⚠️ Authorization Required: The tool execution contains a DROP TABLE statement. Please verify and confirm this action is safe."
        }))
        sys.exit(0)

    print(json.dumps({"decision": "allow"}))

if __name__ == "__main__":
    main()
