import duckdb
import os
import sys

def main():
    _HERE = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(_HERE, "..", "database", "clinisys_all.duckdb"))
    print(f"Connecting to: {db_path}", flush=True)
    try:
        con = duckdb.connect(db_path, read_only=True)
        print("Connected successfully.", flush=True)
        
        # 1. Total row count
        count = con.execute("SELECT COUNT(*) FROM silver.view_pacientes").fetchone()[0]
        print(f"Total rows in silver.view_pacientes: {count:,}", flush=True)
        
        # 2. Schema / columns
        cols = con.execute("DESCRIBE silver.view_pacientes").fetchall()
        print("\nColumns in silver.view_pacientes:", flush=True)
        for col in cols[:15]:
            print(f"  {col[0]}: {col[1]}", flush=True)
        if len(cols) > 15:
            print(f"  ... and {len(cols) - 15} more columns", flush=True)
            
        con.close()
    except Exception as e:
        print(f"Error occurred: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
