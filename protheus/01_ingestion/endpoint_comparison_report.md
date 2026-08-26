# Protheus Endpoints & Schema Comparison Report

**Execution Date:** 2026-08-25T09:00:37.269513

- **Instance A (Huntington):** `https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050` (User: `WSLAKE`)
- **Instance B (BH):** `https://procriar164412.protheus.cloudtotvs.com.br:2157` (User: `WSLAKE`)

## 1. Dynamic Tenants Discovery (`/rest/CONSEMP/empresas`)
- **Huntington Tenants Count:** 18 (`01,010101, 01,010102, 01,010103, 01,010104, 01,010105, 01,010106, 01,010150, 01,010155...`)
- **BH Tenants Count:** 0 (``)
- **Common Tenants (0):** `None`

## 2. Endpoint Connectivity & Performance Matrix
| Endpoint | Type | Status (A) | Status (B) | Latency A (s) | Latency B (s) | Rows A | Rows B | Schema Match % |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `empresas` | full | `200` | `404` | 0.614 | 0.1 | 5 | 0 | Only Instance A |
| `tes` | full | `200` | `404` | 2.165 | 0.159 | 5 | 0 | Only Instance A |
| `produtos` | full | `200` | `404` | 4.487 | 0.165 | 5 | 0 | Only Instance A |
| `clientes` | full | `200` | `404` | 2.015 | 0.115 | 5 | 0 | Only Instance A |
| `vendedores` | full | `200` | `404` | 2.151 | 0.173 | 5 | 0 | Only Instance A |
| `notas` | incremental | `200` | `404` | 5.207 | 0.084 | 5 | 0 | Only Instance A |
| `pedidos` | incremental | `200` | `404` | 5.581 | 0.106 | 5 | 0 | Only Instance A |
| `venda_direta` | incremental | `200` | `404` | 5.532 | 0.172 | 5 | 0 | Only Instance A |

## 3. Detailed Schema & Field Comparison per Endpoint

### Endpoint: `empresas` (`/rest/CONSEMP/empresas`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `tes` (`/rest/CONSTES/tes`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `produtos` (`/rest/CONSPROD/produtos`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `clientes` (`/rest/CONSCLI/clientes`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `vendedores` (`/rest/CONSVEN/vendedores`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `notas` (`/rest/CONSNOTA/notas`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `pedidos` (`/rest/CONSPED/pedidos`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.


### Endpoint: `venda_direta` (`/rest/CONSPEVD/pedidos`)
> [!NOTE]
> Only Instance A returned records. Instance B returned 0 records or errored.
