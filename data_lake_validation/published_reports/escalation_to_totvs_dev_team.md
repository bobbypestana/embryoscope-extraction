# Issues encontrados na API REST Protheus (CONSPED / CONSPEVD / CONSNOTA / CONSCLI / CONSPROD / CONSVEN / CONSTES)

> **Data dos testes:** 2026-06-01 (produção, base
> `huntingtoncentro175132.protheus.cloudtotvs.com.br:4050`)
> **Status:** todos os 5 endpoints listados no e-mail de homologação respondem,
> mas há um conjunto de bugs server-side que precisam ser resolvidos no AdvPL /
> consulta SQL para que a ingestão para o nosso Data Lake funcione de forma
> confiável e incremental.
>
> Esta nota lista cada problema com endpoint afetado, evidência de teste e
> impacto.

---

## 1. Filtro `datetime` ignorado em 4 endpoints (CRÍTICO — bloqueia ingestão incremental)

Endpoints afetados: `/rest/CONSCLI/clientes`, `/rest/CONSPROD/produtos`,
`/rest/CONSVEN/vendedores`, `/rest/CONSTES/tes`.

O parâmetro `datetime` está documentado como filtro a partir da data informada,
mas **não está filtrando**.

**Evidência (CONSCLI/clientes):**

| Requisição | `total` retornado |
| --- | --- |
| sem `datetime` | 149.960 |
| `datetime=1999-01-01 00:00:00.000` | 149.960 |
| `datetime=2026-05-20 00:00:00.000` | 148.998 |
| `datetime=2030-01-01 00:00:00.000` (futuro) | **148.689** |

Mais de 99% dos registros voltam mesmo com data no futuro. Confirma que a query
do endpoint está usando algo equivalente a
`WHERE date_modified >= :datetime OR date_modified IS NULL`. Como praticamente
todos os registros têm `date_modified` vazio, o filtro nunca prune nada.

`CONSTES/tes`: `datetime=2027-01-01` retorna os mesmos 35 registros que sem
filtro nenhum — o filtro é completamente ignorado neste endpoint.

**Impacto:** não conseguimos rodar ingestão incremental nestes endpoints. Toda
execução baixa a tabela inteira mesmo solicitando "desde ontem".

**Solicitamos:** revisar a query backend para usar `WHERE date_modified >= :datetime`
puro (sem `OR IS NULL`), e em paralelo popular o campo de data de modificação
para os registros existentes. Em `/rest/CONSTES/tes` parece que o parâmetro nem
chega ao SQL — investigar handler.

---

## 2. Paginação sem ordenação estável — entrega duplicatas dentro de uma única varredura (CRÍTICO)

Endpoints confirmados afetados até agora: `/rest/CONSCLI/clientes`,
`/rest/CONSTES/tes`. Provavelmente afeta todos.

A paginação via `nPage`/`nPageSize` percorre as páginas mas a ordem dos
registros muda a cada request. Resultado: páginas se sobrepõem e o mesmo
registro aparece em múltiplas páginas, enquanto outros nunca aparecem.

**Evidência 1 — `/rest/CONSCLI/clientes`:** ao paginar duas vezes a página
150 (mesmo `nPageSize=500`) com 30 segundos de intervalo, obtivemos
**diferença simétrica de 100%** — os 1.000 registros eram completamente
diferentes nas duas requisições.

**Evidência 2 — `/rest/CONSTES/tes` (2026-06-01):** com `nPageSize=10`,
paginamos 4 páginas (10 + 10 + 10 + 5 = 35 registros, casando com `total=35`).
Após dedup por chave de negócio (`F4_FILIAL + F4_CODIGO`), só sobraram
**11 registros únicos**. Os outros 24 eram duplicatas do mesmo registro
aparecendo em páginas diferentes.

**Impacto:** mesmo um endpoint pequeno (35 registros) é impossível baixar
sem dedup client-side. Para `clientes` (~150k registros), a probabilidade
de obter cobertura completa em uma varredura é ~64%; precisaríamos rodar
4 varreduras completas para chegar a 98%, multiplicando custo da API por 4.

**Solicitamos:** adicionar `ORDER BY` estável a todas as queries dos
endpoints paginados. Sugestão por endpoint:

| Endpoint | `ORDER BY` sugerido |
| --- | --- |
| CONSCLI/clientes | `A1_FILIAL, A1_COD, A1_LOJA` |
| CONSPROD/produtos | `B1_FILIAL, B1_COD` |
| CONSVEN/vendedores | `A3_FILIAL, A3_COD` |
| CONSTES/tes | `F4_FILIAL, F4_CODIGO` |
| CONSNOTA/notas | `F2_FILIAL, F2_DOC, F2_SERIE` (em conjunto com `dataIni`/`dataFim`) |
| CONSPED/pedidos | `C5_FILIAL, C5_NUM` |
| CONSPEVD/pedidos | `L1_FILIAL, L1_NUM, L1_SERIE` |

---

## 3. Limite oculto de 1000 linhas por consulta em `/rest/CONSNOTA/notas` (CRÍTICO — silenciosamente perde notas)

O endpoint aplica um teto de **1000 linhas de item** por consulta de intervalo
(`dataIni`/`dataFim`). Quando o teto é atingido, o servidor retorna
`hasNext=false` na página seguinte mesmo que ainda existam notas no intervalo
solicitado.

**Evidência (abril/2022, intervalo mensal):**
- API com `dataIni=20220401&dataFim=20220430&nPageSize=500` retorna 2 páginas:
  página 1 = 468 notas / 500 linhas, página 2 = 481 notas / 500 linhas,
  `hasNext=false`. Total visto: 949 notas únicas.
- Total real do mês: **956 notas únicas** (verificado contra
  `silver.mesclada_vendas` legacy).
- Diferença: 7 notas perdidas silenciosamente, incluindo a nota **44293**
  emitida em 29/abr/2022 para Instituto Paulista (R$ 194.369,00).

**Confirmação por janelas menores:** quebrando o mesmo intervalo em 5 janelas
semanais com `nPageSize=100` recupera as 956 notas — incluindo a 44293.

**Mais um exemplo:** junho/2023 — local DB tem 1.500 linhas de item
(1.383 notas únicas); mesma consulta mensal via API só retorna 1.434 notas
únicas, perdendo 51 notas durante o mês movimentado.

**Impacto:** ingestão mensal de notas perde dados de meses com >1000 linhas
de item. Estamos contornando com janelas diárias/semanais, mas isso aumenta
o número de chamadas e não escala para backfill histórico.

**Solicitamos:** remover ou aumentar significativamente o teto de 1000 linhas
(idealmente sem teto, controlando via `nPageSize`), OU adicionar um campo
`truncated=true` na resposta quando o teto for atingido, para que a ingestão
saiba que precisa subdividir a janela. Não retornar `hasNext=false` quando
existirem mais registros.

---

## 4. Filtros documentados não funcionam (`sort_by`, `order`, `ids`)

Tentamos passar parâmetros padrão de REST descritos na nossa especificação
inicial:

- `sort_by=<campo>&order=desc` — ignorado em todos os endpoints (server
  responde 200 mas resultados idênticos ao default).
- `ids=1,2,3` — ignorado (servidor devolve a lista completa, não filtra).
- Filtros custom em `/rest/CONSCLI/clientes` (testamos `cNome`, `cCod`,
  `A1_COD`, `cUF`, `cMun`) — todos silenciosamente ignorados, sempre retornam
  o dataset não filtrado começando em `028393`.

**Impacto:** sem filtros, não temos como pular registros já carregados nem
pegar deltas pontuais.

**Solicitamos:** confirmar quais filtros realmente estão implementados em
cada endpoint, e implementar ao menos um filtro por chave primária
(`?A1_COD=...&A1_LOJA=...` em CONSCLI, `?F2_DOC=...&F2_SERIE=...` em CONSNOTA,
etc.) para podermos rebaixar registros específicos sem refazer a varredura
inteira.

---

## 5. Erro 403 em todos os endpoints para o tenant `05,0101` (Belo Horizonte)

Configuração:
- Usuário: `BRUNO.FEITOSA`
- Header: `TenantId: 05,0101`
- Endpoint testado: todos 7 (CONSNOTA, CONSPED, CONSPEVD, CONSCLI, CONSPROD,
  CONSVEN, CONSTES)
- Resposta: **HTTP 403** em todas as chamadas, retorno rápido (<1s).

Os outros 9 tenants (Ibirapuera, Vila Mariana, Campinas, Pro Fiv, Cenafert,
FIV Brasília) funcionam normalmente com o mesmo usuário.

**Hipóteses:**
1. O usuário não tem permissão para o grupo 05 (BH).
2. O código de filial `0101` está incorreto — note que é mais curto que os
   demais (`010101`, `030101`, `060101` etc.) — pode ser `050101`?

**Solicitamos:** verificar permissão do usuário `BRUNO.FEITOSA` para o tenant
05,0101, OU nos fornecer o formato correto do `TenantId` para Belo Horizonte.

---

## 6. Erros 503 intermitentes em rajadas

Durante a varredura inicial dos 7 endpoints × 10 tenants (70 chamadas
sequenciais com `sleep 0.2s` entre cada), observamos clusters de **HTTP 503
com timeout consistente de ~19s** em alguns endpoints/tenants:

- `/CONSPED/pedidos` para tenants `07,*` (Cenafert e Brasília): 3 falhas
  consecutivas
- `/CONSPEVD/pedidos` para tenants `01,*` e `03,030101`: 5 falhas consecutivas
- `/CONSPROD/produtos` para `07,*`: 3 falhas consecutivas
- `/CONSVEN/vendedores` para `01,*` e `03,030101`: 5 falhas consecutivas
- `/CONSTES/tes` para `07,*`: 3 falhas consecutivas

**Padrão:** as falhas vêm em rajada (varios tenants consecutivos no mesmo
endpoint), depois o endpoint volta a responder OK em tenants posteriores.
Sugere rate-limit ou overload server-side, não problema por tenant.

O response time consistente de exatos ~19s indica timeout interno do servidor
Protheus, não da nossa rede (nosso timeout é 60s, conexão é VPN cabeada).

**Solicitamos:** investigar o que causa as 503s em rajada — pode ser pool de
conexões do AppServer esgotando ou query SQL travando temporariamente.
Implementamos retry com backoff exponencial no nosso ingestor, mas isso só
mascara o problema.

---

## 7. Sem rate-limit headers, sem contrato de erro JSON, sem versionamento

Issues menores mas que dificultam integração robusta:

- Nenhum endpoint retorna headers de rate limit (`X-RateLimit-*`). Não sabemos
  quando estamos próximos do limite ou se existe limite.
- Erros HTTP 500 retornam `{"code":500,"message":"Internal Server Error"}`
  genérico, sem identificar qual campo / qual linha causou o problema.
- Não há prefixo de versionamento (`/api/v1/...`). Mudanças futuras de schema
  vão quebrar nossa ingestão sem aviso.

**Solicitamos (não bloqueante):** adicionar headers de rate-limit, contrato
de erro com `error_code`/`message`/`field`, e prefixo `/api/v1/...` ou
header `Accept: application/vnd.protheus.v1+json`.

---

## Resumo executivo

| # | Problema | Severidade | Bloqueia ingestão? |
| --- | --- | --- | --- |
| 1 | `datetime` filter ignorado em CLI/PROD/VEN/TES | Crítico | Sim — força full load diário |
| 2 | Paginação sem `ORDER BY` estável | Crítico | Sim — dados perdidos em endpoints grandes |
| 3 | Teto oculto de 1000 linhas em CONSNOTA | Crítico | Sim — notas perdidas silenciosamente |
| 4 | Filtros `sort_by`/`ids`/custom ignorados | Alto | Não, mas impede correções pontuais |
| 5 | 403 em BH (`05,0101`) | Médio | Sim para BH |
| 6 | 503s intermitentes em rajada | Médio | Não (mitigado com retry) |
| 7 | Sem rate-limit headers / versionamento | Baixo | Não |

