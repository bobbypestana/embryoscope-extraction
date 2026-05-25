import os
import json
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlencode

# Credentials and connection info
USERNAME = "FILIPE.BELLOTTI"
PASSWORD = os.getenv("PROTHEUS_PASSWORD") or ""
TENANT_ID = "07,030101"
BASE = "https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050"

# Target endpoints and their specific datetime/date parameter configuration
ENDPOINTS = {
    "notas": {
        "path": "/rest/CONSNOTA/notas",
        "date_param_style": "interval",  # dataIni / dataFim (AAAAMMDD)
        "default_params": {"dataIni": "20260101", "dataFim": "20260520", "nPage": 1, "nPageSize": 1}
    },
    "tes": {
        "path": "/rest/CONSTES/tes",
        "date_param_style": "datetime",  # datetime (YYYY-MM-DD HH:MM:SS.mmm)
        "default_params": {"nPage": 1, "nPageSize": 1}
    },
    "produtos": {
        "path": "/rest/CONSPROD/produtos",
        "date_param_style": "datetime",
        "default_params": {"nPage": 1, "nPageSize": 1}
    },
    "clientes": {
        "path": "/rest/CONSCLI/clientes",
        "date_param_style": "datetime",
        "default_params": {"nPage": 1, "nPageSize": 1}
    },
    "vendedores": {
        "path": "/rest/CONSVEN/vendedores",
        "date_param_style": "datetime",
        "default_params": {"nPage": 1, "nPageSize": 1}
    }
}

# Session auth
AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
HEADERS = {
    "TenantId": TENANT_ID,
    "Accept": "application/json"
}

def request_api(url_path, params=None, extra_headers=None, expected_status=200):
    url = f"{BASE}{url_path}"
    headers = HEADERS.copy()
    if extra_headers:
        headers.update(extra_headers)
    
    try:
        r = requests.get(url, params=params, headers=headers, auth=AUTH, timeout=15)
        # Parse JSON if possible
        try:
            body_json = r.json()
        except Exception:
            body_json = None
        
        return {
            "status_code": r.status_code,
            "headers": dict(r.headers),
            "body_raw": r.text,
            "body_json": body_json,
            "success": r.status_code == expected_status
        }
    except Exception as e:
        return {
            "status_code": None,
            "headers": {},
            "body_raw": str(e),
            "body_json": None,
            "success": False,
            "error": str(e)
        }

def run_validation():
    results = {}

    print("======================================================================")
    print("PROTHEUS API VALIDATION SCRIPT")
    print("======================================================================")
    
    # -------------------------------------------------------------------------
    # 1. Versioning & Health Check
    # -------------------------------------------------------------------------
    print("\n--- Testing Versioning & Health Check ---")
    results["health_checks"] = {}
    
    for path in ["/health", "/rest/health", "/api/v1/health"]:
        res = request_api(path)
        print(f"GET {path} -> HTTP {res['status_code']}")
        results["health_checks"][path] = {
            "status_code": res["status_code"],
            "body": res["body_json"] or res["body_raw"][:200]
        }
        if res["status_code"] == 200 and res["body_json"]:
            print(f"  Response: {res['body_json']}")

    # Path versioning: test if /api/v1/ prefix works for an endpoint
    res_v1 = request_api("/api/v1/CONSCLI/clientes", params={"nPage": 1, "nPageSize": 1})
    print(f"GET /api/v1/CONSCLI/clientes -> HTTP {res_v1['status_code']}")
    results["path_versioning"] = {
        "status_code": res_v1["status_code"],
        "success": res_v1["status_code"] == 200
    }

    # -------------------------------------------------------------------------
    # 2. Testing Each Target Endpoint
    # -------------------------------------------------------------------------
    results["endpoints"] = {}
    for ep_name, ep_config in ENDPOINTS.items():
        print(f"\n==========================================")
        print(f"ENDPOINT: {ep_name} ({ep_config['path']})")
        print(f"==========================================")
        
        ep_res = {}
        path = ep_config["path"]
        
        # A. Baseline request with nPageSize=1
        print("\n[A] Baseline check (nPage=1, nPageSize=1)...")
        base_params = ep_config["default_params"].copy()
        res = request_api(path, params=base_params)
        print(f"  HTTP {res['status_code']}")
        
        if res["status_code"] != 200:
            print(f"  Failed baseline request: {res['body_raw'][:500]}")
            results["endpoints"][ep_name] = {"baseline_failed": True, "error": res["body_raw"]}
            continue
            
        ep_res["baseline"] = {
            "status_code": res["status_code"],
            "headers": res["headers"],
            "body_snippet": res["body_raw"][:500]
        }

        # B. Check Envelope Structure
        print("\n[B] Checking Response Envelope...")
        body_json = res["body_json"]
        envelope_check = {
            "is_json": body_json is not None,
            "has_data": False,
            "has_meta": False,
            "has_errors": False,
            "top_level_keys": list(body_json.keys()) if body_json else []
        }
        if body_json:
            envelope_check["has_data"] = "data" in body_json
            envelope_check["has_meta"] = "meta" in body_json
            envelope_check["has_errors"] = "errors" in body_json
            
            # Print the actual keys
            print(f"  Top-level JSON keys: {list(body_json.keys())}")
            # If standard envelope is missing, let's see what is there
            if not envelope_check["has_data"]:
                print("  Missing 'data' key! Actual response layout:")
                # print a small structure check
                for k, v in list(body_json.items())[:3]:
                    val_type = type(v).__name__
                    val_len = len(v) if isinstance(v, (list, dict)) else "N/A"
                    print(f"    - '{k}': type={val_type}, len/val={val_len}")
        else:
            print("  Response is not valid JSON!")
        ep_res["envelope"] = envelope_check

        # C. Pagination Checks
        print("\n[C] Testing Pagination...")
        pagination_check = {}
        
        # Test offset/page-based: nPage=1 vs nPage=2 (nPageSize=1)
        res_page1 = res
        res_page2 = request_api(path, params={**base_params, "nPage": 2, "nPageSize": 1})
        print(f"  nPage=1, nPageSize=1 -> HTTP {res_page1['status_code']}")
        print(f"  nPage=2, nPageSize=1 -> HTTP {res_page2['status_code']}")
        
        if res_page1["status_code"] == 200 and res_page2["status_code"] == 200:
            json1 = res_page1["body_json"]
            json2 = res_page2["body_json"]
            
            # Extract lists of items
            items1 = json1.get("items", []) if isinstance(json1, dict) else []
            items2 = json2.get("items", []) if isinstance(json2, dict) else []
            
            # If items key is not standard, let's try finding list in the top level keys
            if not items1 and isinstance(json1, dict):
                for k, v in json1.items():
                    if isinstance(v, list):
                        items1 = v
                        break
            if not items2 and isinstance(json2, dict):
                for k, v in json2.items():
                    if isinstance(v, list):
                        items2 = v
                        break
            
            if not items1 and isinstance(json1, list):
                items1 = json1
            if not items2 and isinstance(json2, list):
                items2 = json2

            pagination_check["page_based_status"] = "Success"
            pagination_check["page1_count"] = len(items1)
            pagination_check["page2_count"] = len(items2)
            
            if items1 and items2:
                # Compare the first item of each page
                item1_repr = items1[0].get("id") or list(items1[0].values())[0] if isinstance(items1[0], dict) else items1[0]
                item2_repr = items2[0].get("id") or list(items2[0].values())[0] if isinstance(items2[0], dict) else items2[0]
                different = item1_repr != item2_repr
                pagination_check["different_content"] = different
                print(f"  Page 1 first item: {item1_repr}")
                print(f"  Page 2 first item: {item2_repr}")
                print(f"  Pages returned different content? {different}")
            else:
                pagination_check["different_content"] = False
                print("  Could not extract items list for comparison.")
        else:
            pagination_check["page_based_status"] = "Failed"
            
        # Test standard alternative pagination: page=1&page_size=1
        res_alt_page = request_api(path, params={"page": 1, "page_size": 1})
        print(f"  page=1, page_size=1 -> HTTP {res_alt_page['status_code']}")
        pagination_check["alternative_page_params"] = {
            "status_code": res_alt_page["status_code"],
            "success": res_alt_page["status_code"] == 200
        }
        
        # Test cursor-based pagination: limit=1&cursor=abc
        res_cursor = request_api(path, params={"limit": 1, "cursor": "abc"})
        print(f"  limit=1, cursor=abc -> HTTP {res_cursor['status_code']}")
        pagination_check["cursor_params"] = {
            "status_code": res_cursor["status_code"],
            "success": res_cursor["status_code"] == 200
        }
        ep_res["pagination"] = pagination_check

        # D. Sorting Checks
        print("\n[D] Testing Sorting...")
        sorting_check = {}
        # Test sort_by=id&order=desc
        res_sort_desc = request_api(path, params={**base_params, "sort_by": "id", "order": "desc"})
        # Test sort_by=id&order=asc
        res_sort_asc = request_api(path, params={**base_params, "sort_by": "id", "order": "asc"})
        
        print(f"  sort_by=id&order=desc -> HTTP {res_sort_desc['status_code']}")
        print(f"  sort_by=id&order=asc -> HTTP {res_sort_asc['status_code']}")
        
        sorting_check["sort_desc_status"] = res_sort_desc["status_code"]
        sorting_check["sort_asc_status"] = res_sort_asc["status_code"]
        ep_res["sorting"] = sorting_check

        # E. Date Filter Checks
        print("\n[E] Testing Date/Time Filtering...")
        date_check = {}
        
        if ep_config["date_param_style"] == "interval":
            # Testing dataIni and dataFim
            # First, check a valid small interval
            params_valid = {**base_params, "dataIni": "20260501", "dataFim": "20260515"}
            res_valid = request_api(path, params=params_valid)
            print(f"  dataIni=20260501&dataFim=20260515 -> HTTP {res_valid['status_code']}")
            date_check["valid_interval"] = res_valid["status_code"]
            
            # Test ISO-8601 parameters (date_from and date_to)
            params_iso = {**base_params, "date_from": "2026-05-01", "date_to": "2026-05-15"}
            res_iso = request_api(path, params=params_iso)
            print(f"  date_from=2026-05-01&date_to=2026-05-15 -> HTTP {res_iso['status_code']}")
            date_check["iso_interval"] = res_iso["status_code"]
            
        elif ep_config["date_param_style"] == "datetime":
            # Testing datetime parameter
            # Test with datetime=2026-05-01 00:00:00.000
            params_dt = {**base_params, "datetime": "2026-05-01 00:00:00.000"}
            res_dt = request_api(path, params=params_dt)
            print(f"  datetime=2026-05-01 00:00:00.000 -> HTTP {res_dt['status_code']}")
            date_check["valid_datetime"] = res_dt["status_code"]
            
            # Test ISO-8601 alternative date_from
            params_df = {**base_params, "date_from": "2026-05-01"}
            res_df = request_api(path, params=params_df)
            print(f"  date_from=2026-05-01 -> HTTP {res_df['status_code']}")
            date_check["iso_date_from"] = res_df["status_code"]
            
        ep_res["date_filtering"] = date_check

        # F. General Filters Checks
        print("\n[F] Testing General Filters...")
        filters_check = {}
        # Test ids=1,2,3
        res_ids = request_api(path, params={**base_params, "ids": "1,2,3"})
        print(f"  ids=1,2,3 -> HTTP {res_ids['status_code']}")
        filters_check["ids_param"] = res_ids["status_code"]
        
        # Test id=1
        res_id = request_api(path, params={**base_params, "id": "1"})
        print(f"  id=1 -> HTTP {res_id['status_code']}")
        filters_check["id_param"] = res_id["status_code"]
        
        ep_res["general_filters"] = filters_check

        # G. Rate Limiting Headers
        print("\n[G] Checking Rate Limiting Headers...")
        rate_headers = {}
        for k, v in res["headers"].items():
            k_low = k.lower()
            if "ratelimit" in k_low or "limit" in k_low or "remaining" in k_low or "reset" in k_low:
                rate_headers[k] = v
        if rate_headers:
            print(f"  Found rate limit headers: {rate_headers}")
        else:
            print("  No rate limit headers found in response.")
        ep_res["rate_limiting"] = rate_headers

        # H. Error Contracts
        print("\n[H] Testing Error Contracts...")
        error_check = {}
        
        # Test with negative page size or invalid page
        res_err_page = request_api(path, params={**base_params, "nPageSize": -5}, expected_status=400)
        print(f"  nPageSize=-5 -> HTTP {res_err_page['status_code']}")
        error_check["negative_page_size"] = {
            "status_code": res_err_page["status_code"],
            "body_snippet": res_err_page["body_raw"][:300]
        }
        
        # Test with invalid date format
        if ep_config["date_param_style"] == "interval":
            res_err_date = request_api(path, params={**base_params, "dataIni": "invalid-date"}, expected_status=400)
            print(f"  dataIni=invalid-date -> HTTP {res_err_date['status_code']}")
        else:
            res_err_date = request_api(path, params={**base_params, "datetime": "invalid-date"}, expected_status=400)
            print(f"  datetime=invalid-date -> HTTP {res_err_date['status_code']}")
            
        error_check["invalid_date_format"] = {
            "status_code": res_err_date["status_code"],
            "body_snippet": res_err_date["body_raw"][:300]
        }
        ep_res["error_contracts"] = error_check

        results["endpoints"][ep_name] = ep_res

    # Save results to a file
    output_path = "validation_run_results.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults successfully written to {output_path}")

if __name__ == "__main__":
    run_validation()
