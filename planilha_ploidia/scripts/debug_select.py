#!/usr/bin/env python3
"""Check what SQL is actually being executed"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.columns import TARGET_COLUMNS, COLUMN_MAPPING
from utils.db_utils import get_connection, get_available_columns

# Import build_select_clause
from importlib.util import spec_from_file_location, module_from_spec
spec = spec_from_file_location("original", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "01_create_data_ploidia_table.py"))
original_module = module_from_spec(spec)
spec.loader.exec_module(original_module)
build_select_clause = original_module.build_select_clause

conn = get_connection(attach_clinisys=True, read_only=True)
available_columns = get_available_columns(conn)

select_clause, unmapped, missing = build_select_clause(
    TARGET_COLUMNS,
    COLUMN_MAPPING,
    available_columns
)

# Check if select_clause contains DISTINCT
print("Checking SELECT clause...")
print("=" * 80)
print(f"Length: {len(select_clause)} characters")
print(f"First 200 chars: {select_clause[:200]}")
print()

# Count commas (rough estimate of columns)
comma_count = select_clause.count(',')
print(f"Comma count (approx columns): {comma_count}")

conn.close()
