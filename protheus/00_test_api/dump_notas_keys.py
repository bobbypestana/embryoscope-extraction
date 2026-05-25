import os
import requests
from requests.auth import HTTPBasicAuth

USERNAME = "FILIPE.BELLOTTI"
PASSWORD = os.getenv("PROTHEUS_PASSWORD") or ""
TENANT_ID = "07,030101"
BASE = "https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050"

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
HEADERS = {"TenantId": TENANT_ID, "Accept": "application/json"}

def run():
    r = requests.get(f"{BASE}/rest/CONSNOTA/notas", params={"dataIni": "20260519", "dataFim": "20260520", "nPage": 1, "nPageSize": 1}, headers=HEADERS, auth=AUTH)
    if r.status_code == 200:
        data = r.json().get("data", [])
        if data:
            item = data[0]
            print("=== NOTAS KEYS ===")
            print(sorted(list(item.keys())))
            print("=== NOTAS VALUES ===")
            for k in sorted(list(item.keys())):
                print(f"{k}: {repr(item[k])}")
        else:
            print("No data returned")
    else:
        print(f"Error {r.status_code}: {r.text}")

if __name__ == "__main__":
    run()
