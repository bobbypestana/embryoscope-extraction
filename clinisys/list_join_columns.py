import duckdb
import os

# Database paths
source_db_path = os.path.join('..', 'database', 'clinisys_all.duckdb')

# Table mapping
table_mapping = {
    'ooc': ('view_micromanipulacao_oocitos', 'oocito_'),
    'mic': ('view_micromanipulacao', 'micro_'),
    'vce': ('view_congelamentos_embrioes', 'cong_em_'),
    'vde': ('view_descongelamentos_embrioes', 'descong_em_'),
    'vec': ('view_embrioes_congelados', 'emb_cong_'),
    'tr': ('view_tratamentos', 'trat_')
}

with duckdb.connect(source_db_path) as con:
    print("=" * 80)
    print("COLUMNS USED IN THE JOIN (with proper format)")
    print("=" * 80)
    print()
    
    all_columns = []
    
    for alias, (table_name, prefix) in table_mapping.items():
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'silver' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        columns = con.execute(query).df()['column_name'].tolist()
        
        print(f"Table: {table_name} (alias: {alias}, prefix: {prefix})")
        print(f"Total columns: {len(columns)}")
        print("-" * 80)
        
        for col in columns:
            # Format: table_alias.column_name AS prefix_column_name
            formatted = f"{alias}.{col} AS {prefix}{col}"
            all_columns.append(formatted)
            print(f"  {formatted}")
        
        print()
    
    print("=" * 80)
    print(f"TOTAL COLUMNS: {len(all_columns)}")
    print("=" * 80)
    print()
    print("SELECT clause format:")
    print("-" * 80)
    print("SELECT")
    for i, col in enumerate(all_columns, 1):
        comma = "," if i < len(all_columns) else ""
        print(f"    {col}{comma}")

