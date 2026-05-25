import os
import requests
from requests.auth import HTTPBasicAuth

USERNAME = "FILIPE.BELLOTTI"
PASSWORD = os.getenv("PROTHEUS_PASSWORD") or ""
TENANT_ID = "07,030101"
BASE = "https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050"

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
HEADERS = {"TenantId": TENANT_ID, "Accept": "application/json"}

def make_req(path, params):
    try:
        r = requests.get(f"{BASE}{path}", params=params, headers=HEADERS, auth=AUTH, timeout=15)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        return None

def run():
    path_cli = "/rest/CONSCLI/clientes"
    lines = []
    
    # 1. NO datetime
    res_none = make_req(path_cli, {"nPage": 1, "nPageSize": 5})
    lines.append("--- 5 Clientes with NO datetime ---")
    if res_none and "data" in res_none:
        for idx, item in enumerate(res_none["data"]):
            date_fields = {k: v for k, v in item.items() if any(x in k for x in ["DT", "DATA", "MOV", "HR"])}
            lines.append(f"[{idx}] COD={item.get('A1_COD')} NOME={item.get('A1_NOME')[:20]} Dates={date_fields}")
    else:
        lines.append("Failed or empty")
            
    # 2. datetime=2026-05-20 00:00:00.000
    res_dt = make_req(path_cli, {"nPage": 1, "nPageSize": 5, "datetime": "2026-05-20 00:00:00.000"})
    lines.append("\n--- 5 Clientes with datetime=2026-05-20 00:00:00.000 ---")
    if res_dt and "data" in res_dt:
        for idx, item in enumerate(res_dt["data"]):
            date_fields = {k: v for k, v in item.items() if any(x in k for x in ["DT", "DATA", "MOV", "HR"])}
            lines.append(f"[{idx}] COD={item.get('A1_COD')} NOME={item.get('A1_NOME')[:20]} Dates={date_fields}")
    else:
        lines.append("Failed or empty")
            
    # 3. datetime=2030-01-01 00:00:00.000
    res_future = make_req(path_cli, {"nPage": 1, "nPageSize": 5, "datetime": "2030-01-01 00:00:00.000"})
    lines.append("\n--- 5 Clientes with datetime=2030-01-01 00:00:00.000 ---")
    if res_future and "data" in res_future:
        for idx, item in enumerate(res_future["data"]):
            date_fields = {k: v for k, v in item.items() if any(x in k for x in ["DT", "DATA", "MOV", "HR"])}
            lines.append(f"[{idx}] COD={item.get('A1_COD')} NOME={item.get('A1_NOME')[:20]} Dates={date_fields}")
    else:
        lines.append("Failed or empty")

    with open("inspect_dates_output.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    run()
