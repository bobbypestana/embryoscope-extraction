# Protheus Endpoints & Schema Comparison Report

**Execution Date:** 2026-09-04T12:49:41.493648

- **Instance A (Huntington):** `https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050` (User: `WSLAKE`)
- **Instance B (BH / ProCriar):** `https://procriar164412.protheus.cloudtotvs.com.br:2157` (User: `WSLAKE`)

## 1. Dynamic Tenants Discovery (`/rest/CONSEMP/empresas`)
- **Huntington Tenants Count:** 18 (`01,010101, 01,010102, 01,010103, 01,010104, 01,010105, 01,010106, 01,010150, 01,010155...`)
- **BH Tenants Count:** 6 (`01,0101, 01,0102, 01,0103, 05,0101, 05,0201, 05,0301`)
- **Common Tenants (0):** `None`
- **BH Specific Tenants (6):** `01,0101, 01,0102, 01,0103, 05,0101, 05,0201, 05,0301`

## 2. Endpoint Connectivity & Performance Matrix
| Endpoint | Type | Status (A) | Status (B) | Latency A (s) | Latency B (s) | Total A | Total B | Schema Match % |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `empresas` | full | `200` | `200` | 0.652 | 0.495 | 18 | 6 | **100.0%** |
| `tes` | full | `200` | `200` | 1.13 | 0.297 | 36 | 38 | **100.0%** |
| `produtos` | full | `200` | `200` | 0.853 | 0.364 | 9,981 | 1,693 | **97.3%** |
| `clientes` | full | `200` | `200` | 1.853 | 0.368 | 157,936 | 110,276 | **95.7%** |
| `vendedores` | full | `200` | `200` | 0.597 | 0.349 | 323 | 28 | **92.1%** |
| `notas` | incremental | `200` | `200` | 0.571 | 0.494 | 40,731 | 155,149 | **95.5%** |
| `pedidos` | incremental | `200` | `200` | 1.126 | 0.482 | 695 | 27,796 | **92.8%** |
| `venda_direta` | incremental | `200` | `200` | 1.646 | 4.094 | 42,626 | 189,716 | **86.8%** |

## 3. Detailed Schema & Field Comparison per Endpoint

### Endpoint: `empresas` (`/rest/CONSEMP/empresas`)
- **Schema Parity:** **100.0%** match (76 common fields out of 76 in A / 76 in B).
- ✅ **Fields Missing in BH:** None (all Huntington fields are present in BH).
- ✅ **New Fields in BH:** None.

### Endpoint: `tes` (`/rest/CONSTES/tes`)
- **Schema Parity:** **100.0%** match (289 common fields out of 289 in A / 289 in B).
- ✅ **Fields Missing in BH:** None (all Huntington fields are present in BH).
- ✅ **New Fields in BH:** None.

### Endpoint: `produtos` (`/rest/CONSPROD/produtos`)
- **Schema Parity:** **97.3%** match (285 common fields out of 289 in A / 289 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`4` fields)
  ```text
  B1_XGRPCOM, B1_ZCICLOS, B1_ZDGEREN, B1_ZMAPING
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`4` fields)
  ```text
  B1_CTACUS1, B1_ZCODHNT, B1_ZNATURE, B1_ZSTSTES
  ```
- ⚠️ **Field Type Differences:**
  - `B1_PPIS`: Instance A has `int` vs Instance B has `float`

### Endpoint: `clientes` (`/rest/CONSCLI/clientes`)
- **Schema Parity:** **95.7%** match (244 common fields out of 246 in A / 253 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`2` fields)
  ```text
  A1_CBAIRRE, A1_CODMS
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`9` fields)
  ```text
  A1_BAIRFIN, A1_CEPRFIN, A1_CMRFIN, A1_ENDRFIN, A1_MUNRFIN, A1_RESPFIN, A1_UF_RFIN, A1_USARFIN, A1_XDDD
  ```

### Endpoint: `vendedores` (`/rest/CONSVEN/vendedores`)
- **Schema Parity:** **92.1%** match (93 common fields out of 97 in A / 97 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`4` fields)
  ```text
  A3_CRM, A3_XTABELA, A3_ZASSIST, A3_ZTPCR
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`4` fields)
  ```text
  A3_TPPES, A3_ZIRRF, A3_ZISS, A3_ZRTPCC
  ```

### Endpoint: `notas` (`/rest/CONSNOTA/notas`)
- **Schema Parity:** **95.5%** match (231 common fields out of 235 in A / 238 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`4` fields)
  ```text
  F2_LOJAPAC, F2_NOMPACI, F2_XDOC, F2_XNFELET
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`7` fields)
  ```text
  F2_CAIXA, F2_CPFPACI, F2_NOME, F2_NUMCART, F2_SERDES, F2_TIPTITU, F2_UFDESPR
  ```
- ⚠️ **Field Type Differences:**
  - `F2_DESCONT`: Instance A has `float` vs Instance B has `int`
  - `F2_VALISS`: Instance A has `float` vs Instance B has `int`
  - `F2_BASIMP6`: Instance A has `float` vs Instance B has `int`
  - `F2_VALFAT`: Instance A has `float` vs Instance B has `int`
  - `F2_BASIMP5`: Instance A has `float` vs Instance B has `int`
  - `F2_VALBRUT`: Instance A has `float` vs Instance B has `int`
  - `F2_BASEISS`: Instance A has `float` vs Instance B has `int`
  - `F2_VALIMP5`: Instance A has `float` vs Instance B has `int`
- **Nested Structures Comparison:**
  - **`ITENS` Parity:** **98.4%** (300 common fields)
    - Added in BH `ITENS`: `D2_ZVEND1, D2_ZVEND2, D2_ZVEND3, D2_ZVEND4, D2_ZVEND5`

### Endpoint: `pedidos` (`/rest/CONSPED/pedidos`)
- **Schema Parity:** **92.8%** match (155 common fields out of 158 in A / 164 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`3` fields)
  ```text
  C5_NOMCLI, C5_XNOTAAN, C5_XRPSANT
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`9` fields)
  ```text
  C5_ZCODPAC, C5_ZLJPACI, C5_ZNOMECL, C5_ZPACIEN, C5_ZVEND01, C5_ZVEND02, C5_ZVEND03, C5_ZVEND04, C5_ZVEND05
  ```
- **Nested Structures Comparison:**
  - **`ITENS` Parity:** **97.2%** (172 common fields)
    - Added in BH `ITENS`: `C6_ZVEND01, C6_ZVEND02, C6_ZVEND03, C6_ZVEND04, C6_ZVEND05`

### Endpoint: `venda_direta` (`/rest/CONSPEVD/pedidos`)
- **Schema Parity:** **86.8%** match (236 common fields out of 250 in A / 258 in B).
- ⚠️ **Fields Missing in BH (present in Huntington):** (`14` fields)
  ```text
  L1_LOJAPAC, L1_NOMPACI, L1_PACIENT, L1_RESPFIN, L1_ZBAIPS, L1_ZCEPPS, L1_ZCOMPS, L1_ZDMUPS, L1_ZENDPS, L1_ZESTPS, L1_ZMUNPS, L1_ZNUMPS, L1_ZPAIPS, L1_ZREPAS
  ```
- ℹ️ **New Fields Added in BH (not in Huntington):** (`22` fields)
  ```text
  L1_CC, L1_CLIRESP, L1_CPFPACI, L1_LJRESPF, L1_NRESFIN, L1_RECIBO, L1_UNIDADE, L1_USARESP, L1_ZCOMIS1, L1_ZCOMIS2, L1_ZCOMIS3, L1_ZCOMIS4, L1_ZCOMIS5, L1_ZZBAIPS, L1_ZZCEPPS, L1_ZZCOMPS, L1_ZZDMUPS, L1_ZZENDPS, L1_ZZESTPS, L1_ZZMUNPS, L1_ZZNUMPS, L1_ZZPAIPS
  ```
- ⚠️ **Field Type Differences:**
  - `L1_OUTROS`: Instance A has `float` vs Instance B has `int`
  - `L1_VALMERC`: Instance A has `float` vs Instance B has `int`
  - `L1_ENTRADA`: Instance A has `float` vs Instance B has `int`
  - `L1_VLRLIQ`: Instance A has `float` vs Instance B has `int`
  - `L1_VALBRUT`: Instance A has `float` vs Instance B has `int`
  - `L1_VLRTOT`: Instance A has `float` vs Instance B has `int`
  - `L1_VALISS`: Instance A has `float` vs Instance B has `int`
- **Nested Structures Comparison:**
  - **`ITENS` Parity:** **92.1%** (174 common fields)
    - Missing in BH `ITENS`: `L2_DESRPS, L2_NATUREZ, L2_XCODISS, L2_XOPER`
    - Added in BH `ITENS`: `L2_DOCTRAN, L2_VLRCOMI, L2_ZACRES, L2_ZVEND2, L2_ZVEND3, L2_ZVEND4, L2_ZVEND5, L2_ZVLRCM2, L2_ZVLRCM3, L2_ZVLRCM4, L2_ZVLRCM5`
  - **`PAGAMENTO` Parity:** **100.0%** (63 common fields)