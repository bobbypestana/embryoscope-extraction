# Reconciliation Report: Silver Layer vs Local Source Files (Planilha Embriologia)

> [!NOTE]
> ### 📊 Global Summary
> * **Total Local Excel Files Audited**: **32**
> * **Ingested Files**: **29**
> * **Excluded / Skipped Files**: **3**
> * **Total Bronze Rows Loaded**: **115,785**
> * **Total Silver Rows (Fresh + FET)**: **31,278**
> * **Total Unique Silver Patients (Prontuário)**: **14,993**

---

## File-by-File Reconciliation Table

| Year | File Name | Status | Bronze Rows | Bronze Non-Null PIN | Bronze Unique PIN | Silver Fresh Rows | Silver FET Rows | Total Silver Rows | Silver Non-Null Prontuário | Silver Unique Prontuário | Notes |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| 2024 | `CASOS 2024 BH.xlsx` | **INGESTED** | 4,433 | 1,202 | 785 | 537 | 665 | 1,202 | 1,202 | 727 | 3231 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2024 | `CASOS 2024 BSB.xlsx` | **INGESTED** | 6,678 | 469 | 378 | 252 | 217 | 469 | 465 | 312 | 6209 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2024 | `CASOS 2024 IBIRA.xlsx` | **INGESTED** | 7,250 | 1,855 | 1,267 | 924 | 928 | 1,852 | 1,852 | 1,144 | 5398 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2024 | `CASOS 2024 SJ.xlsx` | **INGESTED** | 6,938 | 860 | 609 | 370 | 490 | 860 | 860 | 559 | 6078 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2024 | `CASOS 2024 SSA.xlsx` | **INGESTED** | 4,385 | 629 | 542 | 282 | 347 | 629 | 616 | 401 | 3756 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2024 | `CASOS 2024 VM.xlsx` | **INGESTED** | 6,231 | 1,261 | 901 | 505 | 760 | 1,265 | 1,265 | 864 | 4966 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2025 | `CASOS 2025 BH.xlsx` | **INGESTED** | 4,737 | 1,271 | 832 | 505 | 766 | 1,271 | 1,271 | 765 | 3466 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2025 | `CASOS 2025 BSB.xlsx` | **INGESTED** | 6,241 | 461 | 344 | 218 | 243 | 461 | 460 | 281 | 5780 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2025 | `CASOS 2025 IBI.xlsx` | **INGESTED** | 7,369 | 2,573 | 2,021 | 1,288 | 1,290 | 2,578 | 2,577 | 1,600 | 4791 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2025 | `CASOS 2025 SSA.xlsx` | **INGESTED** | 4,427 | 559 | 484 | 238 | 321 | 559 | 559 | 385 | 3868 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2025 | `CASOS 2025 VM.xlsx` | **INGESTED** | 6,275 | 1,744 | 1,268 | 749 | 998 | 1,747 | 1,742 | 1,143 | 4528 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2022 | `CASOS 2022 BH.xlsx` | **INGESTED** | 1,455 | 40 | 34 | 648 | 678 | 1,326 | 1,326 | 836 | 129 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2022 | `CASOS 2022 BSB.xlsx` | **EXCLUDED / SKIPPED** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Explicitly excluded from ingestion pipeline |
| 2022 | `CASOS 2022 IBI.xlsx` | **EXCLUDED / SKIPPED** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Skipped as duplicate (IBIRA processed instead) |
| 2022 | `CASOS 2022 SJ.xlsx` | **INGESTED** | 1,505 | 1,505 | 1,020 | 684 | 589 | 1,273 | 1,273 | 814 | 232 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2022 | `CASOS 2022 SSA.xlsx` | **INGESTED** | 648 | 4,808 | 524 | 364 | 242 | 606 | 566 | 23 | 42 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2022 | `Casos 2022 VM.xlsx` | **INGESTED** | 2,000 | 2,000 | 1,443 | 856 | 644 | 1,500 | 1,500 | 1,020 | 500 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2023 | `CASOS 2023 BH.xlsx` | **INGESTED** | 1,447 | 1,240 | 926 | 481 | 793 | 1,274 | 1,274 | 744 | 173 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2023 | `CASOS 2023 BSB.xlsx` | **INGESTED** | 3,847 | 144 | 139 | 78 | 0 | 78 | 5 | 5 | 3769 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2023 | `CASOS 2023 IBI.xlsx` | **EXCLUDED / SKIPPED** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Skipped as duplicate (IBIRA processed instead) |
| 2023 | `CASOS 2023 SJ.xlsx` | **INGESTED** | 1,156 | 1,156 | 779 | 490 | 437 | 927 | 927 | 601 | 229 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2023 | `CASOS 2023 SSA.xlsx` | **INGESTED** | 881 | 803 | 697 | 454 | 350 | 804 | 802 | 525 | 77 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2023 | `CASOS 2023 VM.xlsx` | **INGESTED** | 2,034 | 2,034 | 1,367 | 886 | 601 | 1,487 | 1,487 | 960 | 547 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2021 | `CASOS 2021 IBIRA.xlsx` | **INGESTED** | 2,695 | 2,693 | 1,719 | 1,329 | 971 | 2,300 | 2,298 | 1,390 | 395 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2021 | `CASOS 2021 SJ.xlsx` | **INGESTED** | 1,296 | 1,296 | 911 | 601 | 456 | 1,057 | 1,057 | 694 | 239 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2021 | `Casos 2021 VM.xlsx` | **INGESTED** | 2,380 | 2,380 | 1,584 | 1,132 | 641 | 1,773 | 1,773 | 1,146 | 607 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 BH.xlsx` | **INGESTED** | 3,755 | 746 | 559 | 316 | 430 | 746 | 699 | 501 | 3009 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 BSB.xlsx` | **INGESTED** | 6,227 | 274 | 226 | 115 | 156 | 271 | 270 | 204 | 5956 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 IBI.xlsx` | **INGESTED** | 6,315 | 1,569 | 1,339 | 760 | 808 | 1,568 | 1,511 | 1,082 | 4747 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 RIO.xlsx` | **INGESTED** | 3,442 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3442 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 SSA.xlsx` | **INGESTED** | 4,443 | 270 | 247 | 97 | 173 | 270 | 269 | 219 | 4173 rows filtered out in Silver (TIPO filter or empty PIN) |
| 2026 | `CASOS 2026 VM.xlsx` | **INGESTED** | 5,295 | 1,124 | 849 | 473 | 652 | 1,125 | 1,125 | 801 | 4170 rows filtered out in Silver (TIPO filter or empty PIN) |

---

## Key Observations & Lineage Notes

1. **Excluded / Skipped Files**:
   - `CASOS 2022 BSB.xlsx`: Explicitly removed from ingestion per user instructions.
   - `CASOS 2022 IBI.xlsx` & `CASOS 2023 IBI.xlsx`: Automatically skipped during ingestion to prevent duplication because `CASOS 2022 IBIRA.xlsx` and `CASOS 2023 IBIRA.xlsx` provide the consolidated records.

2. **Row Count Transition (Bronze -> Silver)**:
   - Bronze preserves all raw rows from Excel target sheets.
   - Silver applies standardization and categorizes records into `FRESH` and `FET`. Rows with missing PIN or empty/unmatched `TIPO_1` procedures are excluded, resulting in clean silver datasets.

3. **Patient Identification (PIN / Prontuário)**:
   - **Prontuário**: Cleaned, validated integer PIN extracted from raw input string formats (ignoring formatting noise).
   - Unique patient counts are provided at both Fresh, FET, and combined level per source file.
