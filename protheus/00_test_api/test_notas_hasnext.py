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
    r = requests.get(f"{BASE}/rest/CONSNOTA/notas", params={"dataIni": "20240101", "dataFim": "20240131", "nPage": 1, "nPageSize": 200}, headers=HEADERS, auth=AUTH)
    if r.status_code == 200:
        res = r.json()
        print("Root keys:", list(res.keys()))
        for k in res.keys():
            if k != "data":
                print(f"  {k}: {res[k]}")
            else:
                print(f"  data length: {len(res['data'])}")
    else:
        print(f"Error {r.status_code}: {r.text}")

if __name__ == "__main__":
    run()
