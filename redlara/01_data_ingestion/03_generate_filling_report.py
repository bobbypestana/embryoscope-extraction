import duckdb
from pathlib import Path
import importlib.util
import sys

# Paths
BASE_PATH = Path(r"g:\My Drive\projetos_individuais\Huntington\redlara\01_data_ingestion")
DB_PATH = Path(r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb")
SILVER_TABLE = "silver.redlara_unified"
UNIFY_SCRIPT = BASE_PATH / "02_unify_silver.py"
REPORT_PATH = BASE_PATH / "silver_filling_report.txt"

def load_configs():
    # Dynamically import the module with special char/digit name
    spec = importlib.util.spec_from_file_location("mod_unify", str(UNIFY_SCRIPT))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.TABLE_CONFIGS

def generate_report():
    print("Loading configurations...")
    table_configs = load_configs()
    
    conn = duckdb.connect(str(DB_PATH))
    
    # Get all target columns from the first config (assuming uniform keys)
    first_table = list(table_configs.keys())[0]
    target_columns = list(table_configs[first_table]['mapping'].keys())
    
    projected_totals = {}
    
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("SILVER COLUMN FILLING & MAPPING REPORT\n")
        f.write("="*80 + "\n\n")
        
        for col in target_columns:
            f.write(f"COLUMN: {col}\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'BRONZE TABLE':<35} | {'SOURCE COLUMN':<40} | {'FILL RATE':<15}\n")
            f.write("-" * 80 + "\n")
            
            # Aggregate stats across tables
            column_rows = 0
            column_filled = 0
            
            for table_name, config in table_configs.items():
                source_col = config['mapping'].get(col)
                source_display = str(source_col) if source_col else "NONE (NULL)"
                
                filled_count = 0
                rows_count = 0
                
                if source_col:
                    try:
                        # Quote identifier to handle spaces
                        query = f'SELECT count("{source_col}"), count(*) FROM bronze.{table_name}'
                        res = conn.execute(query).fetchone()
                        filled_count = res[0]
                        rows_count = res[1]
                    except Exception as e:
                        source_display += f" [ERROR: {e}]"
                else:
                    # If None, it's NULL in silver. Check total rows of table
                    try:
                        rows_count = conn.execute(f"SELECT count(*) FROM bronze.{table_name}").fetchone()[0]
                    except:
                        pass
                
                pct = (filled_count / rows_count * 100) if rows_count > 0 else 0.0
                
                f.write(f"{table_name:<35} | {source_display:<40} | {filled_count}/{rows_count} ({pct:.1f}%)\n")
                
                column_rows += rows_count
                column_filled += filled_count
            
            total_pct = (column_filled / column_rows * 100) if column_rows > 0 else 0.0
            f.write("-" * 80 + "\n")
            f.write(f"{'TOTAL (Projected)':<35} | {'':<40} | {column_filled}/{column_rows} ({total_pct:.1f}%)\n")
            f.write("\n\n")
            
            projected_totals[col] = column_filled

        # --- SECTION: Final Comparison Summary ---
        f.write("=" * 100 + "\n")
        f.write(f"{'FINAL COMPARISON SUMMARY: PROJECTION VS REALITY':^100}\n")
        f.write("=" * 100 + "\n")
        
        try:
            silver_count = conn.execute(f"SELECT COUNT(*) FROM {SILVER_TABLE}").fetchone()[0]
            f.write(f"Total Rows in Silver Table: {silver_count}\n")
            f.write("-" * 100 + "\n")
            f.write(f"{'COLUMN':<40} | {'PROJECTED (BRONZE)':<20} | {'ACTUAL (SILVER)':<20} | {'MATCH'}\n")
            f.write("-" * 100 + "\n")
            
            for col in target_columns:
                proj = projected_totals.get(col, 0)
                filled = conn.execute(f"SELECT COUNT({col}) FROM {SILVER_TABLE}").fetchone()[0]
                
                # Special handling for 'prontuario' which is populated via lookup, not direct bronze mapping
                if col == 'prontuario':
                    match = "CALC"
                else:
                    match = "✅" if proj == filled else "❌"
                
                f.write(f"{col:<40} | {proj:<20} | {filled:<20} | {match}\n")
            
            f.write("-" * 100 + "\n")
            f.write("Note: 'prontuario' is matched against CliniSys database after unification, so projected is 0.\n")
                
        except Exception as e:
             f.write(f"Error querying silver table for comparison: {e}\n")
            
    conn.close()
    print(f"Consolidated report generated at {REPORT_PATH}")

if __name__ == "__main__":
    generate_report()
