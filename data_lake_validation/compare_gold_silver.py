import os
import duckdb
import pandas as pd
import unicodedata
from collections import defaultdict

# Paths
DB_PATH = 'database/huntington_data_lake.duckdb'
REPORTS_DIR = 'data_lake_validation/reports'
os.makedirs(REPORTS_DIR, exist_ok=True)

def clean_code(code):
    if code is None or pd.isna(code):
        return None
    val = str(code).strip()
    if val.lower() in ('', '0', '-1', '0.0', '-1.0', 'nan', '<na>', 'none', 'null'):
        return None
    # Try converting to float and then int to strip decimals/leading zeros if numeric
    try:
        f = float(val)
        if f.is_integer():
            i = int(f)
            if i in (0, -1):
                return None
            return str(i)
    except ValueError:
        pass
    return val.upper()

def clean_name(name):
    if name is None or pd.isna(name) or not isinstance(name, str):
        return None
    name = name.strip().lower()
    # Normalize accents/unicode
    name = "".join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
    # Keep alphanumeric and spaces
    name = "".join(c if (c.isalnum() or c.isspace()) else "" for c in name)
    name = " ".join(name.split())
    if name.lower() in ('', 'nan', 'none', 'null'):
        return None
    return name

def run_comparison():
    print(f"Connecting to DuckDB at: {DB_PATH}")
    conn = duckdb.connect(DB_PATH)

    # 1. Invoice Comparison
    print("\n--- Running Invoice Comparison ---")
    
    query_invoices = """
        SELECT DISTINCT
            TRY_CAST("Loja" AS INTEGER) AS Loja,
            TRY_CAST("Numero" AS INTEGER) AS Numero,
            "Serie Docto." AS Serie,
            TRY_CAST("DT Emissao" AS DATE) AS DT_Emissao,
            COUNT(*) AS line_items,
            SUM("Total") AS total_amount
        FROM {table}
        WHERE "Grp" != '5' 
          AND "DT Emissao" BETWEEN '2022-01-02' AND '2025-12-15'
          AND "Loja" IS NOT NULL
          AND "Numero" IS NOT NULL
          AND "DT Emissao" IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """

    print("Fetching Gold invoices...")
    df_gold_inv = conn.execute(query_invoices.format(table="gold.protheus_mesclada_vendas")).df()
    print("Fetching Silver invoices...")
    df_silver_inv = conn.execute(query_invoices.format(table="silver.mesclada_vendas")).df()

    print(f"Gold unique invoices count: {len(df_gold_inv):,}")
    print(f"Silver unique invoices count: {len(df_silver_inv):,}")

    # Set keys for comparison
    gold_inv_keys = set(zip(df_gold_inv['Loja'], df_gold_inv['Numero'], df_gold_inv['Serie'], df_gold_inv['DT_Emissao']))
    silver_inv_keys = set(zip(df_silver_inv['Loja'], df_silver_inv['Numero'], df_silver_inv['Serie'], df_silver_inv['DT_Emissao']))

    common_inv_keys = gold_inv_keys.intersection(silver_inv_keys)
    gold_only_inv_keys = gold_inv_keys - silver_inv_keys
    silver_only_inv_keys = silver_inv_keys - gold_inv_keys

    print(f"Common Invoices: {len(common_inv_keys):,}")
    print(f"Gold Only Invoices: {len(gold_only_inv_keys):,}")
    print(f"Silver Only Invoices: {len(silver_only_inv_keys):,}")

    # Save invoice discrepancies to CSV
    df_gold_only_inv = df_gold_inv[df_gold_inv.apply(lambda r: (r['Loja'], r['Numero'], r['Serie'], r['DT_Emissao']) in gold_only_inv_keys, axis=1)]
    df_silver_only_inv = df_silver_inv[df_silver_inv.apply(lambda r: (r['Loja'], r['Numero'], r['Serie'], r['DT_Emissao']) in silver_only_inv_keys, axis=1)]

    # Sort by Date descending and then Numero descending
    df_gold_only_inv = df_gold_only_inv.sort_values(by=['DT_Emissao', 'Numero'], ascending=[False, False])
    df_silver_only_inv = df_silver_only_inv.sort_values(by=['DT_Emissao', 'Numero'], ascending=[False, False])

    gold_only_csv = os.path.join(REPORTS_DIR, 'invoices_gold_only.csv')
    silver_only_csv = os.path.join(REPORTS_DIR, 'invoices_silver_only.csv')
    df_gold_only_inv.to_csv(gold_only_csv, index=False)
    df_silver_only_inv.to_csv(silver_only_csv, index=False)
    print(f"Saved Gold Only invoices to {gold_only_csv}")
    print(f"Saved Silver Only invoices to {silver_only_csv}")

    # 2. Entity comparison (Patients and Clients)
    def compare_entities(entity_name, code_col, name_col, match_by_code=True):
        print(f"\n--- Running {entity_name} Comparison ---")
        
        # Query distinct code/name pairs
        code_select = f'"{code_col}"' if (code_col and match_by_code) else 'NULL'
        query_entities = f"""
            SELECT DISTINCT
                {code_select} AS raw_code,
                "{name_col}" AS raw_name
            FROM {{table}}
            WHERE "Grp" != '5' 
              AND "DT Emissao" BETWEEN '2022-01-02' AND '2025-12-15'
        """
        
        print(f"Fetching Gold {entity_name}s...")
        df_gold_ent = conn.execute(query_entities.format(table="gold.protheus_mesclada_vendas")).df()
        print(f"Fetching Silver {entity_name}s...")
        df_silver_ent = conn.execute(query_entities.format(table="silver.mesclada_vendas")).df()

        # Build graph nodes and edges
        adj = defaultdict(set)
        gold_nodes = set()
        silver_nodes = set()
        raw_info = defaultdict(list)
        unique_names = set()

        def add_record(row, source):
            code = clean_code(row['raw_code']) if match_by_code else None
            name = clean_name(row['raw_name'])
            
            if code is None and name is None:
                return  # Skip empty records
            
            code_node = ('code', code) if code is not None else None
            name_node = ('name', name) if name is not None else None
            
            if name_node:
                unique_names.add(name)
                
            nodes = [n for n in [code_node, name_node] if n is not None]
            
            # Record raw values mapping to these nodes
            for n in nodes:
                raw_info[n].append({'code': row['raw_code'], 'name': row['raw_name'], 'source': source})
                if source == 'gold':
                    gold_nodes.add(n)
                else:
                    silver_nodes.add(n)
                    
            # Connect code and name of this record
            if len(nodes) > 1:
                adj[nodes[0]].add(nodes[1])
                adj[nodes[1]].add(nodes[0])

        print("Processing Gold records...")
        for _, r in df_gold_ent.iterrows():
            add_record(r, 'gold')
            
        print("Processing Silver records...")
        for _, r in df_silver_ent.iterrows():
            add_record(r, 'silver')

        # Prefix name matching for truncated names
        # Only run if there are name nodes
        print("Running prefix name matching for truncated names...")
        sorted_names = sorted(list(unique_names))
        prefix_matches = 0
        for i in range(len(sorted_names) - 1):
            n1 = sorted_names[i]
            # Check subsequent names since there can be multiple close matches
            for j in range(i + 1, min(i + 15, len(sorted_names))):
                n2 = sorted_names[j]
                if n2.startswith(n1) and len(n1) >= 10:
                    node1 = ('name', n1)
                    node2 = ('name', n2)
                    adj[node1].add(node2)
                    adj[node2].add(node1)
                    prefix_matches += 1
        print(f"Added {prefix_matches} edges between prefix-matching names.")

        # Find connected components
        all_nodes = set(adj.keys()).union(gold_nodes).union(silver_nodes)
        visited = set()
        components = []
        
        for node in all_nodes:
            if node not in visited:
                comp = []
                queue = [node]
                visited.add(node)
                while queue:
                    curr = queue.pop(0)
                    comp.append(curr)
                    for nxt in adj[curr]:
                        if nxt not in visited:
                            visited.add(nxt)
                            queue.append(nxt)
                components.append(comp)

        print(f"Total merged {entity_name} entities found: {len(components):,}")

        # Group components by presence
        matched_entities = []
        gold_only_entities = []
        silver_only_entities = []

        for comp in components:
            has_gold = any(n in gold_nodes for n in comp)
            has_silver = any(n in silver_nodes for n in comp)
            
            # Extract raw values for this component
            codes = set()
            names = set()
            raw_details = []
            for n in comp:
                for item in raw_info[n]:
                    raw_details.append(item)
                    c_val = item['code']
                    if c_val is not None and str(c_val).strip() not in ('', '0', '-1', 'None'):
                        codes.add(str(c_val).strip())
                    if item['name'] is not None and str(item['name']).strip() != '' and str(item['name']).strip() != 'None':
                        names.add(str(item['name']).strip())

            # A string summary for code and name
            rep_code = "; ".join(sorted(list(codes))) if codes else "N/A"
            rep_name = "; ".join(sorted(list(names))) if names else "N/A"
            
            entity_data = {
                'representative_code': rep_code,
                'representative_name': rep_name,
                'codes': list(codes),
                'names': list(names),
                'all_records': raw_details
            }

            if has_gold and has_silver:
                matched_entities.append(entity_data)
            elif has_gold:
                gold_only_entities.append(entity_data)
            elif has_silver:
                silver_only_entities.append(entity_data)

        print(f"Matched {entity_name}s: {len(matched_entities):,}")
        print(f"Gold Only {entity_name}s: {len(gold_only_entities):,}")
        print(f"Silver Only {entity_name}s: {len(silver_only_entities):,}")

        # Write to CSV
        def export_to_csv(entities, filename):
            rows = []
            for e in entities:
                # Find the sources of records
                sources = set(item['source'] for item in e['all_records'])
                sources_str = "; ".join(sources)
                rows.append({
                    'Code(s)': e['representative_code'],
                    'Name(s)': e['representative_name'],
                    'Sources': sources_str
                })
            df = pd.DataFrame(rows)
            # Sort by name
            if not df.empty:
                df = df.sort_values(by='Name(s)')
            path = os.path.join(REPORTS_DIR, filename)
            df.to_csv(path, index=False)
            print(f"Saved {len(df):,} records to {path}")
            return df

        df_gold_only = export_to_csv(gold_only_entities, f'{entity_name.lower()}s_gold_only.csv')
        df_silver_only = export_to_csv(silver_only_entities, f'{entity_name.lower()}s_silver_only.csv')

        return {
            'total_gold': len(df_gold_ent),
            'total_silver': len(df_silver_ent),
            'matched': len(matched_entities),
            'gold_only_count': len(gold_only_entities),
            'silver_only_count': len(silver_only_entities),
            'gold_only_samples': df_gold_only.head(15).to_dict(orient='records'),
            'silver_only_samples': df_silver_only.head(15).to_dict(orient='records')
        }

    # Match Patients ONLY by name since code spaces are completely disjoint
    patient_results = compare_entities("Patient", None, "Nom Paciente", match_by_code=False)
    
    # Match Clients by Protheus code (Cliente_totvs) or Name
    client_results = compare_entities("Client", "Cliente_totvs", "Nome", match_by_code=True)

    conn.close()

    return {
        'invoices': {
            'gold_count': len(df_gold_inv),
            'silver_count': len(df_silver_inv),
            'matched': len(common_inv_keys),
            'gold_only_count': len(gold_only_inv_keys),
            'silver_only_count': len(silver_only_inv_keys),
            'gold_only_samples': df_gold_only_inv.head(15).to_dict(orient='records'),
            'silver_only_samples': df_silver_only_inv.head(15).to_dict(orient='records')
        },
        'patients': patient_results,
        'clients': client_results
    }

if __name__ == "__main__":
    import json
    res = run_comparison()
    res_path = os.path.join(REPORTS_DIR, 'comparison_summary.json')
    def default_serializer(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    with open(res_path, 'w') as f:
        json.dump(res, f, indent=4, default=default_serializer)
    print(f"\nSaved comparison summary to {res_path}")
