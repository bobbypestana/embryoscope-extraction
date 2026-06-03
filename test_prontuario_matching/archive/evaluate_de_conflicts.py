import duckdb
import os
import pandas as pd

def main():
    _HERE = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.abspath(os.path.join(_HERE, "..", "..", "database", "test_mapped_patients.duckdb"))
    
    con = duckdb.connect(db_path, read_only=True)
    
    # 1. Count conflicts between D and E where both matched but mapped to different prontuarios
    conflicts_de_both = con.execute("""
        SELECT COUNT(*) FROM main.mapped_patients 
        WHERE prontuario_D != prontuario_E 
          AND prontuario_D != -1 
          AND prontuario_E != -1
    """).fetchone()[0]
    
    # 2. Count total mismatching prontuarios between D and E (including one matched and other unmatched)
    conflicts_de_any = con.execute("""
        SELECT COUNT(*) FROM main.mapped_patients 
        WHERE prontuario_D != prontuario_E
    """).fetchone()[0]
    
    print(f"Conflicts (both matched, different prontuario): {conflicts_de_both}")
    print(f"Conflicts (any difference, including unmatched): {conflicts_de_any}")
    
    # 3. Sample mismatches where both matched to different things
    if conflicts_de_both > 0:
        print("\nSAMPLE MISMATCHES (D vs E where both matched):")
        sample_df = con.execute("""
            SELECT id, name, source, prontuario_D, clinisys_name_D, prontuario_E, clinisys_name_E
            FROM main.mapped_patients
            WHERE prontuario_D != prontuario_E 
              AND prontuario_D != -1 
              AND prontuario_E != -1
            LIMIT 15
        """).df()
        print(sample_df.to_string(index=False))
        
    # 4. Check the 19 conflict cases with E included
    print("\n19 TARGET CONFLICT CASES WITH STRATEGY E:")
    conflict_cases = [
        ("11200", "CANGUSSU, LARISSA PORTO", 611200),
        ("34002", "SILVA, MARIA JOSE", 634002),
        ("34335", "ALVES, RENATA", 634335),
        ("35242", "GARCIA, ANA L Q M", 635242),
        ("56678", "FERREIRA, ANA FLAVIA TEIXEIRA", 656678),
        ("62206", "ROSA, ANA FLAVIA APARECIDA", 662206),
        ("65830", "CAMARGO, ANA CAROLINA FRAGA", 665830),
        ("66058", "MEDIOLI, MARINA", 666058),
        ("69038", "PEREIRA, ANA FLAVIA LOPES", 669038),
        ("77767", "FALLES, LARISSA MAIA CAMPOS", 677767),
        ("56864", "DURAN, ANA CLARA F. L.", 155006),
        ("15941", "", 775941),
        ("51851", "", 150059),
        ("14458", "GABRIELA ROCHA ALBUQUERQUE MADRUGA", 113658),
        ("59666", "LUIZ FERNANDO SIQUEIRA ULHOA CINTRA", 157789),
        ("87229", "MARCELO FALCONE HANAN", 185171),
        ("87229", "THIAGO JARJOUR", 185841),
        ("916778", "LUANA OLIMPIO RIVA", 916778),
        ("916778", "SHUANGWANG JIANG", 784426),
    ]
    
    results = []
    for cid, cname, expected_p in conflict_cases:
        row_db = con.execute("""
            SELECT prontuario_D, clinisys_name_D, prontuario_E, clinisys_name_E
            FROM main.mapped_patients
            WHERE id = ? AND name IS NOT DISTINCT FROM ?
            LIMIT 1
        """, (cid, cname)).fetchone()
        
        if row_db:
            p_d, name_d, p_e, name_e = row_db
            results.append({
                "ID": cid,
                "Name": cname,
                "Expected": expected_p,
                "Got_D": p_d,
                "Got_E": p_e,
                "D_Status": "PASS" if p_d == expected_p else "FAIL",
                "E_Status": "PASS" if p_e == expected_p else "FAIL",
                "Name_D": name_d,
                "Name_E": name_e
            })
            
    df_res = pd.DataFrame(results)
    print(df_res.to_string(index=False))
    
    con.close()

if __name__ == "__main__":
    main()
