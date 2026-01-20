
import duckdb
import pandas as pd
import os
import sys

# Add project root to path to import utils if needed, or just standard imports
# Assuming running from the dir itself

try:
    from column_mapping import COLUMN_MAPPING
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from column_mapping import COLUMN_MAPPING

def get_database_connection(read_only=True):
    db_path = r'g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'
    return duckdb.connect(db_path, read_only=read_only)

def create_planilha_maia():
    print("Connecting to database...")
    con = get_database_connection(read_only=False) # We need write access to create table
    
    try:
        print("Reading source data from gold.planilha_embryoscope_combined...")
        # Read the source table
        df = con.execute("SELECT * FROM gold.planilha_embryoscope_combined").df()
        
        print(f"Source shape: {df.shape}")
        
        # Initialize new dataframe container
        new_data = pd.DataFrame()
        
        # Process Straight Mappings
        print("Processing straight mappings...")
        straight_map = COLUMN_MAPPING.get('straight', {})
        for target_col, source_col in straight_map.items():
            if source_col in df.columns:
                new_data[target_col] = df[source_col]
            else:
                print(f"WARNING: Source column '{source_col}' not found for target '{target_col}'")
                new_data[target_col] = None # Or handle differently
        
        # Process Derived Mappings
        print("Processing derived mappings...")
        derived_map = COLUMN_MAPPING.get('derived', {})
        for target_col, expression in derived_map.items():
            if expression:
                try:
                    # Check if expression is a callable function
                    if callable(expression):
                        # Call the function with the source dataframe
                        new_data[target_col] = expression(df)
                        print(f"  Calculated '{target_col}' using function")
                    elif isinstance(expression, str):
                        # Parse string expression for simple operations
                        if '-' in expression:
                            parts = expression.split('-')
                            col1 = parts[0].strip()
                            col2 = parts[1].strip()
                            if col1 in df.columns and col2 in df.columns:
                                new_data[target_col] = df[col1] - df[col2]
                            else:
                                print(f"WARNING: Source columns '{col1}' or '{col2}' not found for derived '{target_col}'")
                                new_data[target_col] = None
                        elif '+' in expression:
                            parts = expression.split('+')
                            col1 = parts[0].strip()
                            col2 = parts[1].strip()
                            if col1 in df.columns and col2 in df.columns:
                                new_data[target_col] = df[col1] + df[col2]
                            else:
                                print(f"WARNING: Source columns '{col1}' or '{col2}' not found for derived '{target_col}'")
                                new_data[target_col] = None
                        elif '*' in expression:
                            parts = expression.split('*')
                            col1 = parts[0].strip()
                            col2 = parts[1].strip()
                            if col1 in df.columns and col2 in df.columns:
                                new_data[target_col] = df[col1] * df[col2]
                            else:
                                print(f"WARNING: Source columns '{col1}' or '{col2}' not found for derived '{target_col}'")
                                new_data[target_col] = None
                        elif '/' in expression:
                            parts = expression.split('/')
                            col1 = parts[0].strip()
                            col2 = parts[1].strip()
                            if col1 in df.columns and col2 in df.columns:
                                new_data[target_col] = df[col1] / df[col2]
                            else:
                                print(f"WARNING: Source columns '{col1}' or '{col2}' not found for derived '{target_col}'")
                                new_data[target_col] = None
                        else:
                            print(f"WARNING: Unsupported expression format for '{target_col}': {expression}")
                            new_data[target_col] = None
                    else:
                        print(f"WARNING: Invalid type for derived column '{target_col}': {type(expression)}")
                        new_data[target_col] = None
                except Exception as e:
                    print(f"ERROR processing derived column '{target_col}': {e}")
                    import traceback
                    traceback.print_exc()
                    new_data[target_col] = None
        
        # Additional hardcoded derived columns (calculated directly in script)
        print("Calculating additional derived columns...")
        
        # Timing differences
        if 't4-t3' in new_data.columns and new_data['t4-t3'].isna().all():
            if 'embryo_Time_t4' in df.columns and 'embryo_Time_t3' in df.columns:
                new_data['t4-t3'] = df['embryo_Time_t4'] - df['embryo_Time_t3']
                print("  Calculated 't4-t3'")
        
        if 't5-t2' in new_data.columns and new_data['t5-t2'].isna().all():
            if 'embryo_Time_t5' in df.columns and 'embryo_Time_t2' in df.columns:
                new_data['t5-t2'] = df['embryo_Time_t5'] - df['embryo_Time_t2']
                print("  Calculated 't5-t2'")
        
        if 'Tsb-T8' in new_data.columns and new_data['Tsb-T8'].isna().all():
            if 'embryo_Time_tSB' in df.columns and 'embryo_Time_t8' in df.columns:
                new_data['Tsb-T8'] = df['embryo_Time_tSB'] - df['embryo_Time_t8']
                print("  Calculated 'Tsb-T8'")
        
        if 'tB-Tsb' in new_data.columns and new_data['tB-Tsb'].isna().all():
            if 'embryo_Time_tB' in df.columns and 'embryo_Time_tSB' in df.columns:
                new_data['tB-Tsb'] = df['embryo_Time_tB'] - df['embryo_Time_tSB']
                print("  Calculated 'tB-Tsb'")
        
        print(f"New table shape: {new_data.shape}")
        print("Columns:", new_data.columns.tolist())
        
        # Register as view/table in duckdb
        # We will create a new table 'gold.planilha_maia'
        
        # First verify if it exists and drop? Or create table as select?
        # Since we are using pandas to construct potentially, we can use register + create table as
        
        con.register('df_new', new_data)
        
        print("Creating table gold.planilha_maia...")
        con.execute("CREATE OR REPLACE TABLE gold.planilha_maia AS SELECT * FROM df_new")
        
        print("Table gold.planilha_maia created successfully.")
        
        # Verify
        count = con.execute("SELECT COUNT(*) FROM gold.planilha_maia").fetchone()[0]
        print(f"Row count in gold.planilha_maia: {count}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

if __name__ == "__main__":
    create_planilha_maia()
