import os
import sys

# Add this directory to sys.path to import generic_comparator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generic_comparator import run_comparison

def main():
    print("=========================================================")
    print("     EMBRYOSCOPE MULTI-TABLE DATA LAKE RECONCILIATION")
    print("=========================================================\n")
    
    local_db = "database/huntington_data_lake.duckdb"
    athena_db = "silver_embryoscope_staging"
    
    comparisons = [
        {
            "name": "patients",
            "local_table": "silver_embryoscope.patients",
            "athena_table": "patients",
            "keys": ["PatientIDx"],
            "target_keys": ["patient_id_x"],
            "output_name": "embryoscope_patients_reconciliation"
        },
        {
            "name": "treatments",
            "local_table": "silver_embryoscope.treatments",
            "athena_table": "treatments",
            "keys": ["PatientIDx", "TreatmentName"],
            "target_keys": ["patient_id_x", "treatment_name"],
            "output_name": "embryoscope_treatments_reconciliation"
        },
        {
            "name": "embryo_data",
            "local_table": "silver_embryoscope.embryo_data",
            "athena_table": "embryo_data",
            "keys": ["EmbryoID"],
            "target_keys": ["embryo_id"],
            "output_name": "embryoscope_embryo_data_reconciliation"
        }
    ]
    
    results = []
    
    for idx, comp in enumerate(comparisons, 1):
        print(f"[{idx}/3] Reconciling table: {comp['name']}")
        try:
            report_path = run_comparison(
                local_db=local_db,
                local_table=comp["local_table"],
                keys=comp["keys"],
                target_keys=comp["target_keys"],
                athena_db=athena_db,
                athena_table=comp["athena_table"],
                output_name=comp["output_name"]
            )
            results.append({
                "table": comp["name"],
                "status": "Success",
                "report": report_path
            })
        except Exception as e:
            print(f"Error validating table '{comp['name']}': {str(e)}")
            results.append({
                "table": comp["name"],
                "status": "Failed",
                "error": str(e)
            })
            
    print("\n=========================================================")
    print("           RECONCILIATION EXECUTION SUMMARY")
    print("=========================================================")
    
    success_count = sum(1 for r in results if r["status"] == "Success")
    failed_count = sum(1 for r in results if r["status"] == "Failed")
    
    print(f"Total Tables Checked: {len(results)}")
    print(f"  - Passed/Success:  {success_count}")
    print(f"  - Failed:          {failed_count}\n")
    
    print("| Table Name | Execution Status | Report Path / Details |")
    print("| :--- | :--- | :--- |")
    for r in results:
        status_str = f"**{r['status']}**" if r["status"] != "Success" else "Success"
        detail_str = r.get("report", r.get("error", "N/A"))
        if "report" in r:
            abs_path = os.path.abspath(r['report']).replace('\\', '/')
            detail_str = f"[{os.path.basename(r['report'])}](file:///{abs_path})"
        print(f"| {r['table']} | {status_str} | {detail_str} |")

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
