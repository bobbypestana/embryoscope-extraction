import duckdb
import os
import pandas as pd

def main():
    _HERE = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(_HERE, "..", "..", "database", "test_mapped_patients.duckdb"))
    artifact_path = r"C:\Users\FilipeFurlanBellotti\.gemini\antigravity\brain\e198d469-745b-4d13-9fd7-132477c8f2ac\matching_results.md"
    
    print(f"Connecting to: {db_path}")
    con = duckdb.connect(db_path, read_only=True)
    
    # 1. Fetch active conflicts where both D and E matched but to different codes
    df_active = con.execute("""
        SELECT 
            id AS original_id,
            name AS original_name,
            source,
            prontuario_A,
            clinisys_name_A,
            prontuario_B,
            clinisys_name_B,
            prontuario_D,
            clinisys_name_D,
            prontuario_E,
            clinisys_name_E
        FROM main.mapped_patients
        WHERE prontuario_D != prontuario_E 
          AND prontuario_D != -1 
          AND prontuario_E != -1
        ORDER BY id, name
    """).df()
    
    # Fill NAs
    df_active = df_active.fillna("N/A")
    
    print(f"Fetched {len(df_active)} active conflict rows.")
    
    # Read the existing results markdown
    with open(artifact_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Generate the markdown table for active conflicts
    md_table = """
## 5. Detailed D vs E Active Conflicts Report

The following table shows all **{count} rows** where both Strategy D and Strategy E found a match, but disagreed on the prontuario. 

| Original ID | Original Name | Source | Strat A | Name A | Strat B | Name B | Strat D | Name D | Strat E | Name E |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
""".format(count=len(df_active))
    
    for idx, row in df_active.iterrows():
        md_table += "| `{id}` | {name} | {source} | `{p_a}` | {n_a} | `{p_b}` | {n_b} | **`{p_d}`** | **{n_d}** | `{p_e}` | {n_e} |\n".format(
            id=row["original_id"],
            name=row["original_name"],
            source=row["source"],
            p_a=row["prontuario_A"],
            n_a=row["clinisys_name_A"],
            p_b=row["prontuario_B"],
            n_b=row["clinisys_name_B"],
            p_d=row["prontuario_D"],
            n_d=row["clinisys_name_D"],
            p_e=row["prontuario_E"],
            n_e=row["clinisys_name_E"]
        )
        
    # Find Section 5 in the existing results markdown and replace it
    if "## 5. Detailed D vs E Active Conflicts Report" in content:
        base_content = content.split("## 5. Detailed D vs E Active Conflicts Report")[0].strip()
        new_content = base_content + "\n\n" + md_table
    else:
        new_content = content.strip() + "\n\n" + md_table
    
    with open(artifact_path, "w", encoding="utf-8") as f:
        f.write(new_content)
        
    print(f"Successfully updated conflicts report in {artifact_path}")
    con.close()

if __name__ == "__main__":
    main()
