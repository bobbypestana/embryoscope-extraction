import sqlite3
conn = sqlite3.connect(r"g:\My Drive\projetos_individuais\Huntington\db_recovery\files\D2026.02.19_S04797_I3166_P.pdb")
cur = conn.cursor()
cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='IMAGES'")
print(cur.fetchone()[0])
cur.execute("SELECT * FROM IMAGES LIMIT 1")
print([description[0] for description in cur.description])
