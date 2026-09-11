# Unmapped & Schema Variance Report (Huntington vs BH)

**Generated At:** 2026-09-10 22:55:26

This report audits all columns ingested from the two Protheus instances (`bronze` vs `bronze_bh`).

| Table | Total Common Cols | Exclusive to Huntington | Exclusive to BH |
| :--- | :---: | :---: | :---: |
| `notas` | 534 | 6 | 12 |
| `pedidos` | 330 | 7 | 14 |
| `venda_direta` | 413 | 18 | 33 |
| `produtos` | 288 | 3 | 4 |
| `clientes` | 246 | 2 | 9 |
| `vendedores` | 95 | 4 | 4 |
| `tes` | 291 | 0 | 0 |
| `empresas` | 78 | 0 | 0 |

## Detailed Column Breakdown per Table

### Table: `notas`
- **Common Columns (534):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (6):** `D2_MSUIDT, F2_LOJAPAC, F2_MSUIDT, F2_NOMPACI, F2_XDOC, F2_XNFELET`
- ℹ️ **Exclusive to BH (12):** `D2_ZVEND1, D2_ZVEND2, D2_ZVEND3, D2_ZVEND4, D2_ZVEND5, F2_CAIXA, F2_CPFPACI, F2_NOME, F2_NUMCART, F2_SERDES, F2_TIPTITU, F2_UFDESPR`

### Table: `pedidos`
- **Common Columns (330):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (7):** `C5_MSUIDT, C5_NOMCLI, C5_USERLGA, C5_USERLGI, C5_XNOTAAN, C5_XRPSANT, C6_MSUIDT`
- ℹ️ **Exclusive to BH (14):** `C5_ZCODPAC, C5_ZLJPACI, C5_ZNOMECL, C5_ZPACIEN, C5_ZVEND01, C5_ZVEND02, C5_ZVEND03, C5_ZVEND04, C5_ZVEND05, C6_ZVEND01, C6_ZVEND02, C6_ZVEND03, C6_ZVEND04, C6_ZVEND05`

### Table: `venda_direta`
- **Common Columns (413):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (18):** `L1_LOJAPAC, L1_NOMPACI, L1_PACIENT, L1_RESPFIN, L1_ZBAIPS, L1_ZCEPPS, L1_ZCOMPS, L1_ZDMUPS, L1_ZENDPS, L1_ZESTPS, L1_ZMUNPS, L1_ZNUMPS, L1_ZPAIPS, L1_ZREPAS, L2_DESRPS, L2_NATUREZ, L2_XCODISS, L2_XOPER`
- ℹ️ **Exclusive to BH (33):** `L1_CC, L1_CLIRESP, L1_CPFPACI, L1_LJRESPF, L1_NRESFIN, L1_RECIBO, L1_UNIDADE, L1_USARESP, L1_ZCOMIS1, L1_ZCOMIS2, L1_ZCOMIS3, L1_ZCOMIS4, L1_ZCOMIS5, L1_ZZBAIPS, L1_ZZCEPPS, L1_ZZCOMPS, L1_ZZDMUPS, L1_ZZENDPS, L1_ZZESTPS, L1_ZZMUNPS, L1_ZZNUMPS, L1_ZZPAIPS, L2_DOCTRAN, L2_VLRCOMI, L2_ZACRES, L2_ZVEND2, L2_ZVEND3, L2_ZVEND4, L2_ZVEND5, L2_ZVLRCM2, L2_ZVLRCM3, L2_ZVLRCM4, L2_ZVLRCM5`

### Table: `produtos`
- **Common Columns (288):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (3):** `B1_XGRPCOM, B1_ZDGEREN, B1_ZMAPING`
- ℹ️ **Exclusive to BH (4):** `B1_CTACUS1, B1_ZCODHNT, B1_ZNATURE, B1_ZSTSTES`

### Table: `clientes`
- **Common Columns (246):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (2):** `A1_CBAIRRE, A1_CODMS`
- ℹ️ **Exclusive to BH (9):** `A1_BAIRFIN, A1_CEPRFIN, A1_CMRFIN, A1_ENDRFIN, A1_MUNRFIN, A1_RESPFIN, A1_UF_RFIN, A1_USARFIN, A1_XDDD`

### Table: `vendedores`
- **Common Columns (95):** Standard shared Protheus fields.
- ⚠️ **Exclusive to Huntington (4):** `A3_CRM, A3_XTABELA, A3_ZASSIST, A3_ZTPCR`
- ℹ️ **Exclusive to BH (4):** `A3_TPPES, A3_ZIRRF, A3_ZISS, A3_ZRTPCC`

### Table: `tes`
- **Common Columns (291):** Standard shared Protheus fields.
- ✅ **Exclusive to Huntington:** None
- ✅ **Exclusive to BH:** None

### Table: `empresas`
- **Common Columns (78):** Standard shared Protheus fields.
- ✅ **Exclusive to Huntington:** None
- ✅ **Exclusive to BH:** None
