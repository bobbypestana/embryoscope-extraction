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
        else:
            print(f"Error {r.status_code} for {path} with params {params}: {r.text[:200]}")
            return None
    except Exception as e:
        print(f"Exception for {path}: {e}")
        return None

def test_notas_extra():
    print("\n=== Extra Tests for Notas ===")
    path = "/rest/CONSNOTA/notas"
    base_params = {"dataIni": "20260501", "dataFim": "20260515", "nPage": 1, "nPageSize": 2}
    
    # Check sorting
    s_asc = make_req(path, {**base_params, "sort_by": "F2_DOC", "order": "asc"})
    s_desc = make_req(path, {**base_params, "sort_by": "F2_DOC", "order": "desc"})
    if s_asc and s_desc:
        pk_asc = [x.get("F2_DOC") for x in s_asc.get("data", [])]
        pk_desc = [x.get("F2_DOC") for x in s_desc.get("data", [])]
        print(f"Sorting (sort_by=F2_DOC): asc={pk_asc}, desc={pk_desc} (Different? {pk_asc != pk_desc})")
    
    # Check IDs
    f_ids = make_req(path, {**base_params, "ids": "999999,888888"})
    if f_ids:
        pk_ids = [x.get("F2_DOC") for x in f_ids.get("data", [])]
        print(f"IDs filter (?ids=999999,888888): Returned: {pk_ids}")

def test_future_datetime(name, path, pk_field):
    print(f"\n=== Future Datetime Test for {name} ===")
    # Query with a future datetime to see if count drops to 0 (which means filtering works)
    # vs a query with no datetime
    f_none = make_req(path, {"nPage": 1, "nPageSize": 1})
    f_future = make_req(path, {"nPage": 1, "nPageSize": 1, "datetime": "2027-01-01 00:00:00.000"})
    
    if f_none and f_future:
        print(f"None Total: {f_none.get('total')}, Future (2027-01-01) Total: {f_future.get('total')}")
        print(f"  Future Data returned: {f_future.get('data')}")
    else:
        print("Future Datetime: Failed to fetch")

if __name__ == "__main__":
    test_notas_extra()
    test_future_datetime("TES", "/rest/CONSTES/tes", "F4_CODIGO")
    test_future_datetime("Vendedores", "/rest/CONSVEN/vendedores", "A3_COD")
