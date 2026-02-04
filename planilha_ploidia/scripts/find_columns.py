
import sys
import os
import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import get_connection, get_available_columns

def check_columns():
    conn = get_connection(read_only=True)
    try:
        cols = get_available_columns(conn, 'gold.embryoscope_clinisys_combined')
        
        keywords = ['outcome', 'nascidos', 'gravidez', 'resultado', 'clinica', 'fet', 'type', 'merged', 'trat1', 'trat2']
        found = []
        for c in cols:
            if any(k in c.lower() for k in keywords):
                found.append(c)
        
        print("Found columns:")
        for f in sorted(found):
            print(f)
            
    finally:
        conn.close()

if __name__ == "__main__":
    check_columns()
