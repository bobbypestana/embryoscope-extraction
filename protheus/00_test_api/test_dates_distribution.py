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
    
    # We want to find some records that have a non-empty date field.
    # Since only 1,271 records out of 149,960 have non-empty dates, let's query with datetime='2020-01-01 00:00:00.000'
    # which should return all of them.
    # Let's search by changing pages or just look at the metadata.
    # Wait, let's request 100 records and see if we can find any non-empty dates.
    print("Fetching 100 records to inspect date fields...")
    res = make_req(path_cli, {"nPage": 1, "nPageSize": 100, "datetime": "2020-01-01 00:00:00.000"})
    if res and "data" in res:
        non_empty = []
        for item in res["data"]:
            # Check fields like A1_ULTCOM (last purchase), A1_DTCAD (creation date), etc.
            date_info = {k: v for k, v in item.items() if any(x in k for x in ["DT", "DATA", "MOV", "HR"]) and v != ""}
            if date_info:
                non_empty.append((item.get("A1_COD"), item.get("A1_NOME")[:15], date_info))
        
        print(f"Found {len(non_empty)} records with some non-empty date fields out of 100:")
        for code, name, dates in non_empty[:10]:
            print(f"  COD={code} NOME={name} -> {dates}")
    else:
        print("Failed to fetch records")

if __name__ == "__main__":
    run()
