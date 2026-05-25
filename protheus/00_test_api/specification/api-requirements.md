# Requisitos da API

## Requisitos Funcionais

### 1. Versionamento
- `GET /health` — retorna `{ "version": "1.0.0", "status": "ok" }`
- Versão no path: `/api/v1/...` 

### 2. Ordenação
- Query param: `?sort_by=created_at&order=asc|desc`
- Definir quais campos são ordenáveis (nem todos precisam ser)
- Valor padrão explícito: `sort_by=id&order=desc`

### 3. Paginação
**cursor-based** para datasets grandes (mais estável que offset):
- `?limit=50&cursor=<opaque_token>` — resposta inclui `next_cursor`
- Alternativa simples: `?page=1&page_size=50` com `total_count` na resposta

### 4. Filtro por data
- `?date_from=2025-01-01&date_to=2025-12-31` (ISO 8601)
- Definir se é inclusivo nos dois extremos
- Definir qual campo a data se refere: `created_at`, `updated_at`, etc

---

## Requisitos Adicionais

### Filtros gerais
- `?ids=1,2,3` — busca por lista de IDs

### Respostas padronizadas
Envelope consistente para toda a API:
```json
{
  "data": [...],
  "meta": { "page": 1, "total": 230, "limit": 50 },
  "errors": null
}
```

### Rate limiting
- Precisamos da informação de limite de requisições.


### Contratos de erro
- Códigos HTTP corretos para cada cenário
- Body de erro padronizado com `error_code` e `message`

```json
{
  "data": null,
  "errors": {
    "error_code": "INVALID_DATE_RANGE",
    "message": "date_from must be before date_to"
  }
}
```
