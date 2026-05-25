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
    print("=== START CLEAN TEST ===")
    
    # 1. Notas Extra (with date filters)
    path_notas = "/rest/CONSNOTA/notas"
    base_notas = {"dataIni": "20260501", "dataFim": "20260515", "nPage": 1, "nPageSize": 2}
    n_asc = make_req(path_notas, {**base_notas, "sort_by": "F2_DOC", "order": "asc"})
    n_desc = make_req(path_notas, {**base_notas, "sort_by": "F2_DOC", "order": "desc"})
    if n_asc and n_desc:
        pk_asc = [x.get("F2_DOC") for x in n_asc.get("data", [])]
        pk_desc = [x.get("F2_DOC") for x in n_desc.get("data", [])]
        print(f"Notas: sort_by=F2_DOC: asc={pk_asc}, desc={pk_desc} (Different? {pk_asc != pk_desc})")
    else:
        print("Notas: failed to fetch sort tests")
        
    n_ids = make_req(path_notas, {**base_notas, "ids": "000031482,000031483"})
    if n_ids:
        pk_ids = [x.get("F2_DOC") for x in n_ids.get("data", [])]
        print(f"Notas: ids filter returned: {pk_ids} (Filtered? {len(pk_ids) == 2})")
    else:
        print("Notas: failed ids test")

    # 2. TES
    path_tes = "/rest/CONSTES/tes"
    t_none = make_req(path_tes, {"nPage": 1, "nPageSize": 1})
    t_future = make_req(path_tes, {"nPage": 1, "nPageSize": 1, "datetime": "2027-01-01 00:00:00.000"})
    if t_none and t_future:
        print(f"TES: None Total: {t_none.get('total')}, Future Total: {t_future.get('total')} (Filtered? {t_none.get('total') != t_future.get('total')})")
        print(f"  First PK: None={t_none['data'][0].get('F4_CODIGO') if t_none.get('data') else 'N/A'}, Future={t_future['data'][0].get('F4_CODIGO') if t_future.get('data') else 'N/A'}")

    # 3. Produtos
    path_prod = "/rest/CONSPROD/produtos"
    p_none = make_req(path_prod, {"nPage": 1, "nPageSize": 1})
    p_future = make_req(path_prod, {"nPage": 1, "nPageSize": 1, "datetime": "2027-01-01 00:00:00.000"})
    if p_none and p_future:
        print(f"Produtos: None Total: {p_none.get('total')}, Future Total: {p_future.get('total')} (Filtered? {p_none.get('total') != p_future.get('total')})")
        print(f"  First PK: None={p_none['data'][0].get('B1_COD') if p_none.get('data') else 'N/A'}, Future={p_future['data'][0].get('B1_COD') if p_future.get('data') else 'N/A'}")

    # 4. Clientes
    path_cli = "/rest/CONSCLI/clientes"
    c_none = make_req(path_cli, {"nPage": 1, "nPageSize": 1})
    c_future = make_req(path_cli, {"nPage": 1, "nPageSize": 1, "datetime": "2027-01-01 00:00:00.000"})
    if c_none and c_future:
        print(f"Clientes: None Total: {c_none.get('total')}, Future Total: {c_future.get('total')} (Filtered? {c_none.get('total') != c_future.get('total')})")
        print(f"  First PK: None={c_none['data'][0].get('A1_COD') if c_none.get('data') else 'N/A'}, Future={c_future['data'][0].get('A1_COD') if c_future.get('data') else 'N/A'}")

    # 5. Vendedores
    path_ven = "/rest/CONSVEN/vendedores"
    v_none = make_req(path_ven, {"nPage": 1, "nPageSize": 1})
    v_future = make_req(path_ven, {"nPage": 1, "nPageSize": 1, "datetime": "2027-01-01 00:00:00.000"})
    if v_none and v_future:
        print(f"Vendedores: None Total: {v_none.get('total')}, Future Total: {v_future.get('total')} (Filtered? {v_none.get('total') != v_future.get('total')})")
        print(f"  First PK: None={v_none['data'][0].get('A3_COD') if v_none.get('data') else 'N/A'}, Future={v_future['data'][0].get('A3_COD') if v_future.get('data') else 'N/A'}")

if __name__ == "__main__":
    run()
