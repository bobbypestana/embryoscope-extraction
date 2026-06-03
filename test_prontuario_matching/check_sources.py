import duckdb
import os
import pandas as pd

def main():
    _HERE = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(_HERE, "..", "database", "test_mapped_patients.duckdb"))
    
    con = duckdb.connect(db_path, read_only=True)
    
    # Check what columns exist
    info = con.execute("PRAGMA table_info('main.mapped_patients')").df()
    cols = {c.lower() for c in info["name"]}
    
    query_parts = []
    selects = ["source", "COUNT(*) as total"]
    
    if "prontuario_a" in cols:
        selects.append("COUNT(CASE WHEN prontuario_A != -1 THEN 1 END) as matched_A")
    if "prontuario_b" in cols:
        selects.append("COUNT(CASE WHEN prontuario_B != -1 THEN 1 END) as matched_B")
    if "prontuario_d" in cols:
        selects.append("COUNT(CASE WHEN prontuario_D != -1 THEN 1 END) as matched_D")
    if "prontuario_e" in cols:
        selects.append("COUNT(CASE WHEN prontuario_E != -1 THEN 1 END) as matched_E")
        
    query = f"""
        SELECT {', '.join(selects)}
        FROM main.mapped_patients
        GROUP BY source
        ORDER BY source
    """
    
    df = con.execute(query).df()
    print("MATCH COUNTS BY SOURCE:")
    print(df.to_string(index=False))
    
    con.close()

if __name__ == "__main__":
    main()
