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
    res = make_req(path_cli, {"nPage": 1, "nPageSize": 100})
    if res and "data" in res:
        dtcad_values = [item.get("A1_DTCAD") for item in res["data"]]
        non_empty = [v for v in dtcad_values if v and v.strip()]
        print(f"Total DTCAD found: {len(dtcad_values)}")
        print(f"Non-empty DTCAD found: {len(non_empty)}")
        if non_empty:
            print(f"First few non-empty DTCAD: {non_empty[:10]}")
            
        # Let's check another date field, A1_ULTCOM
        ultcom_values = [item.get("A1_ULTCOM") for item in res["data"]]
        non_empty_ult = [v for v in ultcom_values if v and v.strip()]
        print(f"Non-empty ULTCOM found: {len(non_empty_ult)}")
        if non_empty_ult:
            print(f"First few non-empty ULTCOM: {non_empty_ult[:10]}")
    else:
        print("Failed to fetch")

if __name__ == "__main__":
    run()
