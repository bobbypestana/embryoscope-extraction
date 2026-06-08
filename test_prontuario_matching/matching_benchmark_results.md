# Patient Matching Benchmark Results (Strategies A - J)

This document records the match rates, execution timings, and discrepancy analyses for all patient matching strategies tested on the Huntington dataset of **121,560** rows.

## 1. Match Rate & Timing Comparison

The verification suite recreated the test database, ran all strategies, and logged the following summary metrics:

| Strategy | Description | Matched Rows | Total Rows | Match Rate (%) | Execution Time (s) |
|:---:|:---|:---:|:---:|:---:|:---:|
| **A** | Primary exact ID match | 119,497 | 121,560 | 98.30% | - |
| **B** | Secondary exact ID match | 117,745 | 121,560 | 96.86% | 12.5s (A+B combined) |
| **D** | Candidate-based exact ID / birthdate match | 118,883 | 121,560 | 97.80% | 36.3s |
| **E** | Strategy D with tie-breaking rules | 118,883 | 121,560 | 97.80% | 28.5s |
| **G** | E with dynamic schema column discovery | 120,198 | 121,560 | 98.88% | 18.8s |
| **H** | Unified SQL exact first-name match | 120,447 | 121,560 | 99.08% | 22.3s |
| **I** | H with spelling tolerance (Levenshtein <= 1) | 120,809 | 121,560 | 99.38% | 63.7s |
| **J** | **Optimized spelling tolerance (Pre-Normalized)** | **120,809** | **121,560** | **99.38%** | **28.0s** |

## 2. Key Verification Findings

### Strategy I vs. Strategy J Match Parity
* **Parity Check**: A SQL-level comparison query was run:
  ```sql
  SELECT COUNT(*) FROM main.mapped_patients WHERE prontuario_I != prontuario_J;
  ```
  Result: **0 discrepancies**. The match outputs of the optimized Strategy J are **100% identical** to Strategy I.
* **Speedup**: Shifting the phonetic name normalization logic from an on-the-fly macro evaluation during joins down to a one-time pre-normalization step during temp table preparation reduced the execution time from **63.7s** to **28.0s** (**2.3× speedup**).

### Spelling Recovery & Accuracy
* Reclaiming spelling-tolerant matches (e.g. `Chintia` $\rightarrow$ `Cínthia` under prontuario `838919`, `PRISCILLA` $\rightarrow$ `Prisila`, `Lobo, FERNADA G` $\rightarrow$ `Fernanda Gomes Lobo`) yielded **362 newly matched correct patients** compared to Strategy H, with **zero false positive mismatches** due to the custom phonetic normalization and gender safeguards.

### Reference Conflict Cases
* **19/19 Passed** for all E, G, H, I, and J strategies, confirming zero regression on hard cases.
* Confirmed that `CAMILA DA HORA PASSOS` (ID `792194`, CPF `02639455532`) is successfully matched under both Strategy I and Strategy J.
