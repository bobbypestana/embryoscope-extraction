#!/usr/bin/env python3
"""
Compare Protheus API Endpoints and Schemas: Huntington vs BH
Probes all 8 endpoints across both instances, verifies HTTP behavior,
discovers tenants, extracts sample schemas (including nested structures),
and generates a comparison report in Markdown and JSON.
"""

import os
import sys
import yaml
import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple
import requests
from requests.auth import HTTPBasicAuth

# Setup logging
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'compare_protheus_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load parameters
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

API_CONF = config['api']

# Config for Instance A (Huntington)
INST_A = {
    "name": "Huntington (Current)",
    "base_url": API_CONF['base_url'].rstrip('/'),
    "username": API_CONF['username'],
    "password": API_CONF['password'],
    "tenant_id": API_CONF.get('tenant_id', '07,030101'),
    "bootstrap_tenant": "07,030101"
}

# Config for Instance B (BH)
INST_B = {
    "name": "BH (New)",
    "base_url": API_CONF['base_url_bh'].rstrip('/'),
    "username": API_CONF.get('username_bh', API_CONF['username']),
    "password": API_CONF['password_bh'],
    "tenant_id": API_CONF.get('tenant_id_bh', '05,010101'),
    "bootstrap_tenant": "05,010101"
}

def create_session(auth: HTTPBasicAuth) -> requests.Session:
    """Create a fresh session for Protheus to properly release thread slots on close."""
    s = requests.Session()
    s.auth = auth
    return s

def make_request(instance: Dict[str, Any], path: str, params: Optional[Dict] = None, tenant_id: Optional[str] = None, timeout: int = 60) -> Dict[str, Any]:
    """
    Executes an HTTP GET with Protheus best practices (Connection: close, fresh session per tenant/call).
    Returns a dict with status_code, elapsed_seconds, response_json, error, headers.
    """
    auth = HTTPBasicAuth(instance['username'], instance['password'])
    session = create_session(auth)
    
    url = f"{instance['base_url']}{path}"
    headers = {
        "Accept": "application/json",
        "Connection": "close"
    }
    if tenant_id:
        headers["TenantId"] = tenant_id

    max_attempts = 3
    result = {
        "url": url,
        "path": path,
        "params": params,
        "tenant_id": tenant_id,
        "status_code": None,
        "elapsed_seconds": None,
        "data": None,
        "error": None,
        "headers": {}
    }

    try:
        for attempt in range(1, max_attempts + 1):
            start_time = time.time()
            try:
                r = session.get(url, params=params, headers=headers, timeout=timeout)
                elapsed = time.time() - start_time
                result["status_code"] = r.status_code
                result["elapsed_seconds"] = round(elapsed, 3)
                result["headers"] = dict(r.headers)

                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    try:
                        result["data"] = r.json()
                    except Exception as json_err:
                        result["error"] = f"JSON decode error: {json_err}. Body preview: {r.text[:300]}"
                    return result
                elif r.status_code in [400, 403, 404]:
                    result["error"] = f"HTTP {r.status_code}: {r.text[:500]}"
                    return result
                else:
                    result["error"] = f"HTTP {r.status_code}: {r.text[:300]}"
                    logger.warning(f"[{instance['name']}] Attempt {attempt}/{max_attempts} for {path} failed with HTTP {r.status_code}. Retrying...")
            except Exception as req_err:
                elapsed = time.time() - start_time
                result["elapsed_seconds"] = round(elapsed, 3)
                result["error"] = str(req_err)
                logger.warning(f"[{instance['name']}] Attempt {attempt}/{max_attempts} for {path} failed with error: {req_err}")

            if attempt < max_attempts:
                sleep_time = 2 * attempt + random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
    finally:
        session.close()

    return result

def extract_schema(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extracts unified schema structure across multiple sample records:
    - field name -> type description (e.g. 'str', 'int', 'float', 'list[dict]', 'dict')
    """
    if not records:
        return {}
    
    schema = {}
    nested_records_map = {}

    for record in records:
        if not isinstance(record, dict):
            continue
        for k, v in record.items():
            if k not in schema or schema[k] == "null":
                if v is None:
                    schema[k] = "null"
                elif isinstance(v, bool):
                    schema[k] = "bool"
                elif isinstance(v, int):
                    schema[k] = "int"
                elif isinstance(v, float):
                    schema[k] = "float"
                elif isinstance(v, str):
                    schema[k] = "str"
                elif isinstance(v, list):
                    if len(v) > 0 and isinstance(v[0], dict):
                        schema[k] = "list[dict]"
                        if k not in nested_records_map:
                            nested_records_map[k] = []
                        nested_records_map[k].extend(v)
                    elif len(v) > 0:
                        schema[k] = f"list[{type(v[0]).__name__}]"
                    else:
                        schema[k] = "list[empty]"
                elif isinstance(v, dict):
                    schema[k] = "dict"
                    if k not in nested_records_map:
                        nested_records_map[k] = []
                    nested_records_map[k].append(v)
                else:
                    schema[k] = type(v).__name__

    # Process nested structures
    for nested_k, nested_list in nested_records_map.items():
        schema[f"{nested_k}__nested_schema"] = extract_schema(nested_list)

    return schema

def compare_schemas(schema_a: Dict[str, Any], schema_b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares two schemas and identifies matching fields, missing in B, extra in B, and type differences.
    """
    keys_a = {k for k in schema_a.keys() if not k.endswith('__nested_schema')}
    keys_b = {k for k in schema_b.keys() if not k.endswith('__nested_schema')}

    common_keys = keys_a.intersection(keys_b)
    missing_in_b = sorted(list(keys_a - keys_b))
    extra_in_b = sorted(list(keys_b - keys_a))

    type_mismatches = {}
    for k in common_keys:
        type_a = schema_a.get(k)
        type_b = schema_b.get(k)
        if type_a != type_b and type_a != "null" and type_b != "null":
            type_mismatches[k] = {"inst_a": type_a, "inst_b": type_b}

    # Compare nested schemas if present
    nested_diffs = {}
    for k in common_keys:
        nested_a_key = f"{k}__nested_schema"
        nested_b_key = f"{k}__nested_schema"
        if nested_a_key in schema_a and nested_b_key in schema_b:
            nested_diffs[k] = compare_schemas(schema_a[nested_a_key], schema_b[nested_b_key])
        elif nested_a_key in schema_a and nested_b_key not in schema_b:
            nested_diffs[k] = {"status": f"Nested array '{k}' present in A but missing or empty in B"}
        elif nested_b_key in schema_a and nested_a_key not in schema_a:
            nested_diffs[k] = {"status": f"Nested array '{k}' present in B but missing or empty in A"}

    total_keys = len(keys_a.union(keys_b))
    match_pct = (len(common_keys) / total_keys * 100) if total_keys > 0 else 100.0

    return {
        "keys_count_a": len(keys_a),
        "keys_count_b": len(keys_b),
        "common_keys_count": len(common_keys),
        "match_percentage": round(match_pct, 1),
        "missing_in_b": missing_in_b,
        "extra_in_b": extra_in_b,
        "type_mismatches": type_mismatches,
        "nested_diffs": nested_diffs,
        "schema_a_keys": sorted(list(keys_a)),
        "schema_b_keys": sorted(list(keys_b))
    }

def run_comparison():
    logger.info("================================================================================")
    logger.info("Starting Protheus Endpoint & Schema Comparison (Huntington vs BH)")
    logger.info(f"Instance A: {INST_A['name']} @ {INST_A['base_url']}")
    logger.info(f"Instance B: {INST_B['name']} @ {INST_B['base_url']}")
    logger.info("================================================================================")

    report_summary = {
        "timestamp": datetime.now().isoformat(),
        "instances": {
            "instance_a": {"name": INST_A["name"], "url": INST_A["base_url"], "user": INST_A["username"]},
            "instance_b": {"name": INST_B["name"], "url": INST_B["base_url"], "user": INST_B["username"]}
        },
        "tenants": {
            "instance_a": [],
            "instance_b": []
        },
        "endpoints": {}
    }

    # Step 1: Discover Tenants via /rest/CONSEMP/empresas
    logger.info("\n--- STEP 1: Discovering Tenants (/rest/CONSEMP/empresas) ---")
    emp_res_a = make_request(INST_A, "/rest/CONSEMP/empresas", params={"nPage": 1, "nPageSize": 50}, tenant_id=INST_A["bootstrap_tenant"])
    emp_res_b = make_request(INST_B, "/rest/CONSEMP/empresas", params={"nPage": 1, "nPageSize": 50}, tenant_id=INST_B["bootstrap_tenant"])

    tenants_a = []
    tenants_b = []

    if emp_res_a.get("data") and isinstance(emp_res_a["data"], dict) and "data" in emp_res_a["data"]:
        items = emp_res_a["data"]["data"]
        tenants_a = [f"{item.get('M0_CODIGO', '').strip()},{item.get('M0_CODFIL', '').strip()}" for item in items if item.get('M0_CODIGO') and item.get('M0_CODFIL')]
        logger.info(f"[{INST_A['name']}] Found {len(tenants_a)} tenants: {tenants_a}")
    else:
        logger.warning(f"[{INST_A['name']}] Could not list tenants from /rest/CONSEMP/empresas. Error: {emp_res_a.get('error')}")

    if emp_res_b.get("data") and isinstance(emp_res_b["data"], dict) and "data" in emp_res_b["data"]:
        items = emp_res_b["data"]["data"]
        tenants_b = [f"{item.get('M0_CODIGO', '').strip()},{item.get('M0_CODFIL', '').strip()}" for item in items if item.get('M0_CODIGO') and item.get('M0_CODFIL')]
        logger.info(f"[{INST_B['name']}] Found {len(tenants_b)} tenants: {tenants_b}")
    else:
        logger.warning(f"[{INST_B['name']}] Could not list tenants from /rest/CONSEMP/empresas. Error: {emp_res_b.get('error')}")

    report_summary["tenants"]["instance_a"] = tenants_a
    report_summary["tenants"]["instance_b"] = tenants_b

    # Select representative tenant for tenant-scoped calls
    tenant_for_a = INST_A['tenant_id'] if INST_A['tenant_id'] in tenants_a else (tenants_a[0] if tenants_a else "07,030101")
    tenant_for_b = "05,010101" # ProCriar BH valid tenant

    logger.info(f"Using Tenant for test calls: Instance A -> '{tenant_for_a}', Instance B -> '{tenant_for_b}'")

    # Define endpoints to test
    date_today = datetime.now()
    data_fim = date_today.strftime("%Y%m%d")

    endpoint_configs = [
        {
            "name": "empresas",
            "path": "/rest/CONSEMP/empresas",
            "type": "full",
            "params": {"nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "tes",
            "path": "/rest/CONSTES/tes",
            "type": "full",
            "params": {"nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "produtos",
            "path": "/rest/CONSPROD/produtos",
            "type": "full",
            "params": {"nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "clientes",
            "path": "/rest/CONSCLI/clientes",
            "type": "full",
            "params": {"nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "vendedores",
            "path": "/rest/CONSVEN/vendedores",
            "type": "full",
            "params": {"nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "notas",
            "path": "/rest/CONSNOTA/notas",
            "type": "incremental",
            "params": {"dataIni": "20220101", "dataFim": data_fim, "nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "pedidos",
            "path": "/rest/CONSPED/pedidos",
            "type": "incremental",
            "params": {"dataIni": "20220101", "dataFim": data_fim, "nPage": 1, "nPageSize": 10},
            "use_tenant": True
        },
        {
            "name": "venda_direta",
            "path": "/rest/CONSPEVD/pedidos",
            "type": "incremental",
            "params": {"dataIni": "20220101", "dataFim": data_fim, "nPage": 1, "nPageSize": 10},
            "use_tenant": True
        }
    ]

    # Step 2: Probe and compare each endpoint
    logger.info("\n--- STEP 2: Testing & Comparing All 8 Endpoints ---")

    for ep in endpoint_configs:
        ep_name = ep["name"]
        path = ep["path"]
        params = ep["params"]
        t_a = tenant_for_a if ep["use_tenant"] else None
        t_b = tenant_for_b if ep["use_tenant"] else None

        logger.info(f"\nTesting endpoint [{ep_name.upper()}] ({path}) ...")
        res_a = make_request(INST_A, path, params=params, tenant_id=t_a)
        res_b = make_request(INST_B, path, params=params, tenant_id=t_b)

        ep_report = {
            "name": ep_name,
            "path": path,
            "type": ep["type"],
            "params_used": params,
            "instance_a": {
                "status_code": res_a["status_code"],
                "elapsed_seconds": res_a["elapsed_seconds"],
                "error": res_a["error"],
                "records_returned": 0,
                "has_next": None,
                "total": None,
                "schema": {}
            },
            "instance_b": {
                "status_code": res_b["status_code"],
                "elapsed_seconds": res_b["elapsed_seconds"],
                "error": res_b["error"],
                "records_returned": 0,
                "has_next": None,
                "total": None,
                "schema": {}
            },
            "schema_comparison": {}
        }

        # Extract record sample & schema from A
        records_a = []
        if res_a.get("data") and isinstance(res_a["data"], dict):
            records_a = res_a["data"].get("data", [])
            ep_report["instance_a"]["records_returned"] = len(records_a) if isinstance(records_a, list) else 0
            ep_report["instance_a"]["has_next"] = res_a["data"].get("hasNext")
            ep_report["instance_a"]["total"] = res_a["data"].get("total")
            if isinstance(records_a, list) and len(records_a) > 0:
                ep_report["instance_a"]["schema"] = extract_schema(records_a)
                ep_report["instance_a"]["sample_record"] = records_a[0]

        # Extract record sample & schema from B
        records_b = []
        if res_b.get("data") and isinstance(res_b["data"], dict):
            records_b = res_b["data"].get("data", [])
            ep_report["instance_b"]["records_returned"] = len(records_b) if isinstance(records_b, list) else 0
            ep_report["instance_b"]["has_next"] = res_b["data"].get("hasNext")
            ep_report["instance_b"]["total"] = res_b["data"].get("total")
            if isinstance(records_b, list) and len(records_b) > 0:
                ep_report["instance_b"]["schema"] = extract_schema(records_b)
                ep_report["instance_b"]["sample_record"] = records_b[0]

        # If both schemas extracted, compare them
        if ep_report["instance_a"]["schema"] and ep_report["instance_b"]["schema"]:
            ep_report["schema_comparison"] = compare_schemas(
                ep_report["instance_a"]["schema"],
                ep_report["instance_b"]["schema"]
            )
        elif ep_report["instance_a"]["schema"] and not ep_report["instance_b"]["schema"]:
            ep_report["schema_comparison"] = {
                "status": "Only Instance A returned records. Instance B returned 0 records or errored.",
                "missing_in_b": sorted(list(ep_report["instance_a"]["schema"].keys())),
                "extra_in_b": []
            }
        elif not ep_report["instance_a"]["schema"] and ep_report["instance_b"]["schema"]:
            ep_report["schema_comparison"] = {
                "status": "Only Instance B returned records. Instance A returned 0 records or errored.",
                "missing_in_b": [],
                "extra_in_b": sorted(list(ep_report["instance_b"]["schema"].keys()))
            }
        else:
            ep_report["schema_comparison"] = {
                "status": "No records returned on either instance."
            }

        report_summary["endpoints"][ep_name] = ep_report
        logger.info(f"[{ep_name}] Inst A Status: {res_a['status_code']} ({res_a['elapsed_seconds']}s, {ep_report['instance_a']['records_returned']} rows, total: {ep_report['instance_a']['total']}) | Inst B Status: {res_b['status_code']} ({res_b['elapsed_seconds']}s, {ep_report['instance_b']['records_returned']} rows, total: {ep_report['instance_b']['total']})")

    # Step 3: Write JSON Output
    json_path = os.path.join(SCRIPT_DIR, 'endpoint_comparison_diff.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report_summary, f, indent=2, ensure_ascii=False)
    logger.info(f"\nJSON report written to: {json_path}")

    # Step 4: Write Markdown Report
    md_path = os.path.join(SCRIPT_DIR, 'endpoint_comparison_report.md')
    generate_markdown_report(report_summary, md_path)
    logger.info(f"Markdown report written to: {md_path}")

    return report_summary

def generate_markdown_report(summary: Dict[str, Any], output_path: str):
    inst_a = summary["instances"]["instance_a"]
    inst_b = summary["instances"]["instance_b"]
    tenants_a = summary["tenants"]["instance_a"]
    tenants_b = summary["tenants"]["instance_b"]

    lines = []
    lines.append("# Protheus Endpoints & Schema Comparison Report")
    lines.append(f"\n**Execution Date:** {summary['timestamp']}")
    lines.append(f"\n- **Instance A (Huntington):** `{inst_a['url']}` (User: `{inst_a['user']}`)")
    lines.append(f"- **Instance B (BH / ProCriar):** `{inst_b['url']}` (User: `{inst_b['user']}`)")

    # Section: Tenants
    lines.append("\n## 1. Dynamic Tenants Discovery (`/rest/CONSEMP/empresas`)")
    lines.append(f"- **Huntington Tenants Count:** {len(tenants_a)} (`{', '.join(tenants_a[:8]) + ('...' if len(tenants_a) > 8 else '')}`)")
    lines.append(f"- **BH Tenants Count:** {len(tenants_b)} (`{', '.join(tenants_b[:8]) + ('...' if len(tenants_b) > 8 else '')}`)")
    
    tenants_common = set(tenants_a).intersection(set(tenants_b))
    tenants_only_a = set(tenants_a) - set(tenants_b)
    tenants_only_b = set(tenants_b) - set(tenants_a)

    lines.append(f"- **Common Tenants ({len(tenants_common)}):** `{', '.join(sorted(list(tenants_common))) if tenants_common else 'None'}`")
    if tenants_only_b:
        lines.append(f"- **BH Specific Tenants ({len(tenants_only_b)}):** `{', '.join(sorted(list(tenants_only_b)))}`")

    # Section: Matrix
    lines.append("\n## 2. Endpoint Connectivity & Performance Matrix")
    lines.append("| Endpoint | Type | Status (A) | Status (B) | Latency A (s) | Latency B (s) | Total A | Total B | Schema Match % |")
    lines.append("| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for ep_name, ep in summary["endpoints"].items():
        st_a = ep["instance_a"]["status_code"] or "ERR"
        st_b = ep["instance_b"]["status_code"] or "ERR"
        lat_a = ep["instance_a"]["elapsed_seconds"] if ep["instance_a"]["elapsed_seconds"] is not None else "-"
        lat_b = ep["instance_b"]["elapsed_seconds"] if ep["instance_b"]["elapsed_seconds"] is not None else "-"
        tot_a = f"{ep['instance_a']['total']:,}" if ep['instance_a']['total'] is not None else str(ep['instance_a']['records_returned'])
        tot_b = f"{ep['instance_b']['total']:,}" if ep['instance_b']['total'] is not None else str(ep['instance_b']['records_returned'])
        
        comp = ep.get("schema_comparison", {})
        match_pct = f"{comp.get('match_percentage', 0.0)}%" if "match_percentage" in comp else comp.get("status", "N/A")[:15]

        lines.append(f"| `{ep_name}` | {ep['type']} | `{st_a}` | `{st_b}` | {lat_a} | {lat_b} | {tot_a} | {tot_b} | **{match_pct}** |")

    # Section: Detailed Differences per Endpoint
    lines.append("\n## 3. Detailed Schema & Field Comparison per Endpoint")

    for ep_name, ep in summary["endpoints"].items():
        lines.append(f"\n### Endpoint: `{ep_name}` (`{ep['path']}`)")
        comp = ep.get("schema_comparison", {})
        
        if not comp:
            lines.append("- No comparison available.")
            continue

        if "status" in comp:
            lines.append(f"> [!NOTE]\n> {comp['status']}\n")

        if "match_percentage" in comp:
            lines.append(f"- **Schema Parity:** **{comp['match_percentage']}%** match ({comp['common_keys_count']} common fields out of {comp['keys_count_a']} in A / {comp['keys_count_b']} in B).")

            if comp["missing_in_b"]:
                lines.append(f"- ⚠️ **Fields Missing in BH (present in Huntington):** (`{len(comp['missing_in_b'])}` fields)")
                lines.append(f"  ```text\n  {', '.join(comp['missing_in_b'])}\n  ```")
            else:
                lines.append("- ✅ **Fields Missing in BH:** None (all Huntington fields are present in BH).")

            if comp["extra_in_b"]:
                lines.append(f"- ℹ️ **New Fields Added in BH (not in Huntington):** (`{len(comp['extra_in_b'])}` fields)")
                lines.append(f"  ```text\n  {', '.join(comp['extra_in_b'])}\n  ```")
            else:
                lines.append("- ✅ **New Fields in BH:** None.")

            if comp.get("type_mismatches"):
                lines.append("- ⚠️ **Field Type Differences:**")
                for field, types in comp["type_mismatches"].items():
                    lines.append(f"  - `{field}`: Instance A has `{types['inst_a']}` vs Instance B has `{types['inst_b']}`")

            if comp.get("nested_diffs"):
                lines.append("- **Nested Structures Comparison:**")
                for nested_name, n_diff in comp["nested_diffs"].items():
                    if "match_percentage" in n_diff:
                        lines.append(f"  - **`{nested_name}` Parity:** **{n_diff.get('match_percentage')}%** ({n_diff.get('common_keys_count')} common fields)")
                        if n_diff.get("missing_in_b"):
                            lines.append(f"    - Missing in BH `{nested_name}`: `{', '.join(n_diff['missing_in_b'])}`")
                        if n_diff.get("extra_in_b"):
                            lines.append(f"    - Added in BH `{nested_name}`: `{', '.join(n_diff['extra_in_b'])}`")
                    elif "status" in n_diff:
                        lines.append(f"  - **`{nested_name}`:** {n_diff['status']}")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

if __name__ == "__main__":
    run_comparison()
