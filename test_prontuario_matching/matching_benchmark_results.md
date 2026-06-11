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
| **I** | H with spelling tolerance (Levenshtein <= 1) | 120,809 | 121,560 | 99.38% | 63.7s (original run) / 44.7s |
| **J** | Optimized spelling tolerance (Pre-Normalized) | 120,809 | 121,560 | 99.38% | 20.6s |
| **L** | **Pre-Normalized with Spelling & Couple Fallbacks** | **120,863** | **121,560** | **99.43%** | **23.1s** |

## 2. Key Verification Findings

### Strategy J Parity & Strategy L Fallback Recovery
* **Strategy J Parity**: A SQL-level comparison query verified that Strategy J match outputs are **100% identical** to Strategy I but execute in less than half the time (20.6s vs 44.7s).
* **Strategy L Recoveries**: Strategy L successfully matched **120,863 rows** (a **99.43%** match rate), recovering **54 new correct matches** compared to Strategy J/I.
  * **Spelling Fallback (Levenshtein = 2)**: Safely recovered transpositions like `Nelia` vs `Neila` under `GRANDO, NELIA B. S.` and `Daneila` vs `Daniela` under `Metello, Daneila Gomes` when the last name matched exactly.
  * **Couple Folder Fallback**: Allowed spousal matches on direct ID matches (`source_id = codigo`) if the Jaro-Winkler similarity is `>= 0.72` and they share at least 2 non-preposition words (e.g. `GABRIELA BRANCO SONEGO` matching `Joao Gabriel Branco Sonego` in folder `220979`), while successfully rejecting false positives like child `Camila Santos` vs mother `Jocileide Santos`.
* **Speedup**: Strategy L completed in **23.1 seconds** (a **1.9× speedup** over Strategy I).

### Spelling Recovery & Accuracy
* Reclaiming spelling-tolerant matches (e.g. `Chintia` $\rightarrow$ `Cínthia` under prontuario `838919`, `PRISCILLA` $\rightarrow$ `Prisila`, `Lobo, FERNADA G` $\rightarrow$ `Fernanda Gomes Lobo`) yielded **362 newly matched correct patients** compared to Strategy H, with **zero false positive mismatches** due to the custom phonetic normalization and gender safeguards.

### Reference Conflict Cases
* **19/19 Passed** for all E, G, H, I, and J strategies, confirming zero regression on hard cases.
* Confirmed that `CAMILA DA HORA PASSOS` (ID `792194`, CPF `02639455532`) is successfully matched under both Strategy I and Strategy J.
