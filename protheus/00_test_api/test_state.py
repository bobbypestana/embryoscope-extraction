import os
import requests
from requests.auth import HTTPBasicAuth
import time

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

def test_session_state():
    path_cli = "/rest/CONSCLI/clientes"
    print("=== TESTING STATEFUL / DATETIME BEHAVIOR FOR CLIENTES ===")
    
    # 1. First request with no datetime
    res1 = make_req(path_cli, {"nPage": 1, "nPageSize": 1})
    print(f"Request 1 (no datetime) -> Total: {res1.get('total') if res1 else 'Failed'}")
    
    # 2. Second request with no datetime immediately after
    res2 = make_req(path_cli, {"nPage": 1, "nPageSize": 1})
    print(f"Request 2 (no datetime) -> Total: {res2.get('total') if res2 else 'Failed'}")
    
    # 3. Third request with datetime = 1999-01-01 00:00:00.000
    res3 = make_req(path_cli, {"nPage": 1, "nPageSize": 1, "datetime": "1999-01-01 00:00:00.000"})
    print(f"Request 3 (datetime=1999-01-01) -> Total: {res3.get('total') if res3 else 'Failed'}")
    
    # 4. Fourth request with datetime = 2026-05-20 00:00:00.000
    res4 = make_req(path_cli, {"nPage": 1, "nPageSize": 1, "datetime": "2026-05-20 00:00:00.000"})
    print(f"Request 4 (datetime=2026-05-20 00:00:00.000) -> Total: {res4.get('total') if res4 else 'Failed'}")

    # 5. Fifth request with datetime = 2026-05-20 18:00:00.000
    res5 = make_req(path_cli, {"nPage": 1, "nPageSize": 1, "datetime": "2026-05-20 18:00:00.000"})
    print(f"Request 5 (datetime=2026-05-20 18:00:00.000) -> Total: {res5.get('total') if res5 else 'Failed'}")

    # 6. Sixth request with a crazy future datetime
    res6 = make_req(path_cli, {"nPage": 1, "nPageSize": 1, "datetime": "2030-01-01 00:00:00.000"})
    print(f"Request 6 (datetime=2030-01-01) -> Total: {res6.get('total') if res6 else 'Failed'}")

if __name__ == "__main__":
    test_session_state()
