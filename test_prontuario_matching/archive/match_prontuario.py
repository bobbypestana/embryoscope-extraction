"""
match_prontuario.py
===================
Generic Strategy C patient prontuario matching for DuckDB.

Enriches any table in-place with two columns:
  - prontuario   BIGINT   (-1 = unmatched)
  - clinisys_name VARCHAR  (best candidate name from clinisys_all)

Only rows where prontuario IS NULL or = -1 are processed, so the function is
safe to call repeatedly (already-matched rows are preserved).

Column discovery
----------------
At runtime, the function introspects clinisys_all.silver.view_pacientes and
groups its columns automatically:

  * Columns whose name contains 'esposa'      -> esposa group  (priority 2)
  * Columns whose name contains 'marido'      -> marido group  (priority 3)
  * Columns whose name contains 'responsavel' -> responsavel group (priority 4)

Within each group:
  - prontuario_* columns -> used in the IN (...) join condition
  - *_nome column        -> used for name scoring and as the matched name

New clinic-specific columns (e.g. prontuario_esposa_rio) added to the view
in the future are picked up automatically without any code changes.

Usage
-----
    import duckdb
    from match_prontuario import match_prontuario

    con = duckdb.connect("huntington_data_lake.duckdb")

    stats = match_prontuario(
        source_con=con,
        db_path="clinisys_all.duckdb",
        source_schema="silver",
        source_table="my_table",
        id_col="PatientID",
        name_col="PatientName",   # optional -- omit to match by ID only
        label="my_table",         # optional -- used in log messages
    )
    # stats -> {"total": N, "matched": M, "unmatched": U, "rate": 0.XX,
    #           "groups": [{"role": "esposa", "prontuario_cols": [...], ...}, ...]}

    # Inspect what was discovered:
    # stats["groups"]

Notes
-----
- All matching logic lives in SQL (a single UPDATE ... FROM (...) statement).
- Python only: attaches the clinisys DB, introspects its columns, ensures the
  two output columns exist, generates the SQL, and reports stats.
- Word arrays for clinisys names are computed once in the clinisys_processed
  CTE and referenced by alias everywhere.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

import duckdb

logger = logging.getLogger(__name__)


def _t(label: str, start: float) -> None:
    """Log elapsed time in ms since `start`."""
    logger.info("  [T] %-40s %6.0f ms", label, (time.perf_counter() - start) * 1000)

_CLINISYS_ALIAS = "clinisys_all"
_CLINISYS_VIEW  = f"{_CLINISYS_ALIAS}.silver.view_pacientes"

# Ordered list of roles; the order determines priority_group (2, 3, 4, ...)
_ROLE_KEYWORDS = ["esposa", "marido", "responsavel"]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class MatchGroup:
    """Represents one clinisys prontuario role (esposa / marido / responsavel)."""
    role:            str            # e.g. 'esposa'
    priority:        int            # 2, 3, 4, ...
    nome_col:        Optional[str]  # e.g. 'esposa_nome'  (None if absent)
    prontuario_cols: List[str] = field(default_factory=list)  # all c.prontuario_* columns

    @property
    def match_type(self) -> str:
        return f"{self.role}_match"

    @property
    def words_alias(self) -> str:
        return f"{self.role}_words"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _attach_clinisys(con: duckdb.DuckDBPyConnection, db_path: str, tag: str) -> None:
    """Attach clinisys_all.duckdb (idempotent)."""
    t0 = time.perf_counter()
    try:
        con.execute(f"ATTACH '{db_path}' AS {_CLINISYS_ALIAS} (READ_ONLY)")
        _t(f"[{tag}] ATTACH clinisys_all", t0)
        logger.info("[%s] Attached '%s' as '%s'", tag, db_path, _CLINISYS_ALIAS)
    except Exception as exc:
        if "already attached" not in str(exc).lower():
            raise
        _t(f"[{tag}] ATTACH clinisys_all (already attached)", t0)
        logger.debug("[%s] '%s' already attached", tag, _CLINISYS_ALIAS)


def _ensure_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> None:
    """Add prontuario (BIGINT, default -1) and clinisys_name (VARCHAR) if absent."""
    t0 = time.perf_counter()
    info     = con.execute(f"PRAGMA table_info('{schema}.{table}')").df()
    existing = {c.lower() for c in info["name"]}

    if "prontuario" not in existing:
        logger.info("[match_prontuario] Adding 'prontuario' -> %s.%s", schema, table)
        con.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN prontuario BIGINT')
        con.execute(f'UPDATE {schema}."{table}" SET prontuario = -1')

    if "clinisys_name" not in existing:
        logger.info("[match_prontuario] Adding 'clinisys_name' -> %s.%s", schema, table)
        con.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN clinisys_name VARCHAR')
    _t(f"ensure_columns ({schema}.{table})", t0)


def _discover_groups(con: duckdb.DuckDBPyConnection) -> List[MatchGroup]:
    """
    Introspect view_pacientes and return one MatchGroup per role keyword,
    populated with whatever columns are actually present in the view.

    Prontuario columns  -> any column whose name contains both 'prontuario'
                           and the role keyword.
    Name column         -> the column whose name contains both the role keyword
                          and 'nome' (first match wins; None if absent).
    """
    t0 = time.perf_counter()
    rows = con.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog = '{_CLINISYS_ALIAS}' "
        f"  AND table_schema  = 'silver' "
        f"  AND table_name    = 'view_pacientes'"
    ).fetchall()
    all_cols = [r[0] for r in rows]
    _t("information_schema.columns query", t0)

    groups = []
    for priority, role in enumerate(_ROLE_KEYWORDS, start=2):
        prontuario_cols = [
            c for c in all_cols
            if role in c.lower() and "prontuario" in c.lower()
        ]
        nome_col = next(
            (c for c in all_cols if role in c.lower() and "nome" in c.lower()),
            None,
        )
        g = MatchGroup(
            role            = role,
            priority        = priority,
            nome_col        = nome_col,
            prontuario_cols = prontuario_cols,
        )
        groups.append(g)
        logger.info(
            "[match_prontuario] Discovered group '%s' (priority %d): "
            "nome_col=%s, prontuario_cols=%s",
            role, priority, nome_col, prontuario_cols,
        )

    return groups


def _word_array_sql(col_expr: str) -> str:
    """
    DuckDB expression: normalize a name string into an array of lowercase
    accent-stripped alphabetic tokens.  e.g. 'Joao, Silva' -> ['joao','silva']
    """
    return (
        f"list_filter(\n"
        f"            regexp_split_to_array(\n"
        f"                regexp_replace(strip_accents(lower(trim({col_expr}))), '[^a-z]', ' ', 'g'),\n"
        f"                '\\s+'\n"
        f"            ),\n"
        f"            w -> length(w) > 0\n"
        f"        )"
    )


def _score_sql(p_words: str, c_words: str) -> str:
    """
    DuckDB CASE expression scoring name similarity between p_words and c_words.

    1 -- exact match
    2 -- first + last word both present
    3 -- first word + >=1 other word present
    4 -- first word present only
    5 -- first word absent  (-> rejected; score < 5 required)
    """
    return (
        f"CASE\n"
        f"            WHEN {p_words} IS NULL OR {c_words} IS NULL\n"
        f"              OR len({p_words}) = 0  OR len({c_words}) = 0    THEN 5\n"
        f"            WHEN {p_words} = {c_words}                        THEN 1\n"
        f"            WHEN NOT list_contains({c_words}, {p_words}[1])   THEN 5\n"
        f"            WHEN list_contains({c_words}, {p_words}[-1])      THEN 2\n"
        f"            WHEN len(list_filter({p_words},\n"
        f"                 w -> list_contains({c_words}, w))) > 1       THEN 3\n"
        f"            ELSE 4\n"
        f"        END"
    )


# ---------------------------------------------------------------------------
# SQL builders
# ---------------------------------------------------------------------------

def _build_clinisys_cte(groups: List[MatchGroup]) -> str:
    """
    Build the clinisys_processed CTE.  Selects:
      - codigo, inativo
      - *_nome columns for all groups that have one
      - pre-computed *_words arrays for name scoring
      - all prontuario link columns discovered per group
    """
    select_parts = ["codigo", "inativo"]

    for g in groups:
        if g.nome_col:
            select_parts.append(g.nome_col)
            select_parts.append(
                f"{_word_array_sql(g.nome_col)} AS {g.words_alias}"
            )
        else:
            # Provide a NULL placeholder so downstream SQL is uniform
            select_parts.append(
                f"CAST(NULL AS VARCHAR)   AS {g.role}_nome"
            )
            select_parts.append(
                f"CAST(NULL AS VARCHAR[]) AS {g.words_alias}"
            )
        for col in g.prontuario_cols:
            select_parts.append(col)

    cols_sql = ",\n            ".join(select_parts)
    return f"""
    clinisys_processed AS (
        SELECT
            {cols_sql}
        FROM {_CLINISYS_VIEW}
    )"""


def _build_group1_cte(groups: List[MatchGroup], has_name: bool) -> str:
    """
    Group 1: direct codigo match.
    Scores against ALL nome columns; keeps the best-matching name.
    """
    if has_name and any(g.nome_col for g in groups):
        score_parts  = []
        name_cases   = []
        for g in groups:
            if g.nome_col:
                score_expr = _score_sql("s.p_words", f"c.{g.words_alias}")
                score_parts.append(score_expr)
                name_cases.append((score_expr, f"c.{g.nome_col}"))

        # LEAST(score_esposa, score_marido, ...)
        score_m1 = "LEAST(\n            " + ",\n            ".join(score_parts) + "\n        )"

        # CASE WHEN best_score THEN nome ELSE ... END
        # Build as a chain: compare each pair
        if len(name_cases) == 1:
            matched_name_m1 = f"c.{groups[0].nome_col}"
        else:
            # Pick nome whose score is minimum
            cases = []
            for i, (score_i, nome_i) in enumerate(name_cases[:-1]):
                others = " AND ".join(
                    f"({score_i}) <= ({score_j})"
                    for j, (score_j, _) in enumerate(name_cases)
                    if j != i
                )
                cases.append(f"WHEN {others} THEN {nome_i}")
            last_nome = name_cases[-1][1]
            matched_name_m1 = "CASE\n            " + "\n            ".join(cases) + f"\n            ELSE {last_nome}\n        END"
    else:
        score_m1 = "1"
        nome_cols = [f"c.{g.nome_col}" for g in groups if g.nome_col]
        matched_name_m1 = f"COALESCE({', '.join(nome_cols)})" if nome_cols else "CAST(NULL AS VARCHAR)"

    return f"""
    matches_1 AS (
        SELECT
            s.source_id,
            s.p_words,
            s.original_prontuario,
            c.codigo       AS prontuario,
            1              AS priority_group,
            c.inativo,
            {score_m1}     AS name_score,
            {matched_name_m1} AS matched_name,
            'codigo_main'  AS match_type
        FROM source_extract s
        INNER JOIN clinisys_processed c
            ON TRY_CAST(s.source_id AS BIGINT) = c.codigo
    )"""


def _build_group_cte(g: MatchGroup, has_name: bool) -> str:
    """Build a matches_N CTE for one role group (esposa / marido / responsavel)."""
    if not g.prontuario_cols:
        return ""   # nothing to join on -- skip this group

    in_list = ",\n                ".join(f"c.{col}" for col in g.prontuario_cols)

    if has_name and g.nome_col:
        score = _score_sql("s.p_words", f"c.{g.words_alias}")
    else:
        score = "1"

    nome_expr = f"c.{g.nome_col}" if g.nome_col else "CAST(NULL AS VARCHAR)"

    cte_name = f"matches_{g.priority}"
    return f"""
    {cte_name} AS (
        SELECT
            s.source_id,
            s.p_words,
            s.original_prontuario,
            c.codigo           AS prontuario,
            {g.priority}       AS priority_group,
            c.inativo,
            {score}            AS name_score,
            {nome_expr}        AS matched_name,
            '{g.match_type}'   AS match_type
        FROM source_extract s
        INNER JOIN clinisys_processed c
            ON TRY_CAST(s.source_id AS BIGINT) IN (
                {in_list}
            )
    )"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_prontuario(
    source_con:    duckdb.DuckDBPyConnection,
    db_path:       str,
    source_schema: str,
    source_table:  str,
    id_col:        str,
    name_col:      Optional[str] = None,
    label:         str = "",
) -> dict:
    """
    Run Strategy C prontuario matching and update the source table in-place.

    Parameters
    ----------
    source_con    : Open DuckDB connection to the database that holds source_table.
    db_path       : Filesystem path to clinisys_all.duckdb.
    source_schema : Schema that contains source_table (e.g. 'silver', 'gold').
    source_table  : Name of the table to enrich.
    id_col        : Column in source_table whose values are patient IDs to match
                    against clinisys prontuario codes.
    name_col      : Optional column with patient names.
                    When provided, matches are scored by name similarity and
                    low-quality matches (score = 5) are rejected.
                    When omitted, any ID hit is accepted.
    label         : Human-readable tag for log messages.

    Returns
    -------
    dict with keys:
        total    (int)         -- total rows in table
        matched  (int)         -- rows with prontuario != -1
        unmatched (int)
        rate     (float 0-1)
        groups   (list[dict])  -- discovered clinisys groups with their columns
    """
    tag = label or f"{source_schema}.{source_table}"
    t_total = time.perf_counter()
    logger.info("[%s] === Starting match_prontuario ===", tag)

    # 1. Attach clinisys DB
    _attach_clinisys(source_con, db_path, tag)

    # 2. Discover column groups from view_pacientes
    t0 = time.perf_counter()
    groups = _discover_groups(source_con)
    _t(f"[{tag}] discover_groups (total)", t0)

    # 3. Ensure output columns exist on target table
    _ensure_columns(source_con, source_schema, source_table)

    # 3b. Count rows to process
    t0 = time.perf_counter()
    pending = source_con.execute(
        f'SELECT COUNT(*) FROM {source_schema}."{source_table}" '
        f'WHERE prontuario IS NULL OR prontuario = -1'
    ).fetchone()[0]
    _t(f"[{tag}] count pending rows", t0)
    logger.info("[%s] Rows to match: %s", tag, pending)

    has_name = bool(name_col)

    # 4. Source name parsing fragment (handles "LASTNAME, FIRSTNAME" format)
    if has_name:
        parsed_name_sql = f"""
        CASE
            WHEN s."{name_col}" IS NOT NULL THEN
                CASE
                    WHEN POSITION(',' IN s."{name_col}") > 0
                        THEN SPLIT_PART(s."{name_col}", ',', 2)
                          || ' '
                          || SPLIT_PART(s."{name_col}", ',', 1)
                    ELSE s."{name_col}"
                END
            ELSE NULL
        END"""
        p_words_sql = _word_array_sql(parsed_name_sql)
        name_select = f's."{name_col}"'
    else:
        p_words_sql = "CAST(NULL AS VARCHAR[])"
        name_select = "CAST(NULL AS VARCHAR)"

    # 5. Assemble CTEs
    clinisys_cte = _build_clinisys_cte(groups)
    group1_cte   = _build_group1_cte(groups, has_name)

    role_ctes        = []
    all_match_names  = ["matches_1"]
    for g in groups:
        cte = _build_group_cte(g, has_name)
        if cte:
            role_ctes.append(cte)
            all_match_names.append(f"matches_{g.priority}")

    union_sql = "\n    UNION ALL ".join(
        f"SELECT * FROM {name}" for name in all_match_names
    )

    matching_query = f"""
    WITH

    -- Step 1: distinct unmatched source IDs
    source_extract AS (
        SELECT DISTINCT
            CAST(s."{id_col}" AS VARCHAR)  AS source_id,
            {name_select}                  AS patient_name,
            {p_words_sql}                  AS p_words,
            s.prontuario                   AS original_prontuario
        FROM {source_schema}."{source_table}" s
        WHERE s."{id_col}" IS NOT NULL
          AND CAST(s."{id_col}" AS VARCHAR) != ''
          AND (s.prontuario IS NULL OR s.prontuario = -1)
    ),

    -- Step 2: clinisys reference -- columns discovered at runtime
    {clinisys_cte},

    -- Step 3: Group 1 -- direct codigo match (highest priority)
    {group1_cte},

    -- Steps 4+: one CTE per role group (esposa / marido / responsavel / ...)
    {chr(10).join('    ' + c + ',' for c in role_ctes)}

    -- Merge all groups
    all_matches AS (
        {union_sql}
    ),

    -- Rank by quality within each (source_id, p_words) partition
    ranked_matches AS (
        SELECT
            source_id,
            p_words,
            prontuario,
            matched_name,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, p_words
                ORDER BY
                    priority_group                                     ASC,
                    name_score                                         ASC,
                    CASE WHEN prontuario = original_prontuario
                         THEN 0 ELSE 1 END                            ASC,
                    CASE WHEN inativo = '0' THEN 0 ELSE 1 END         ASC,
                    prontuario                                         DESC
            ) AS rn
        FROM all_matches
        WHERE name_score < 5    -- reject when source first-name token is absent
    )

    -- Left join back to source_extract (strategy_c.sql pattern):
    -- every pending row is returned; unmatched rows get prontuario = -1.
    SELECT
        s.source_id,
        COALESCE(r.prontuario, -1) AS prontuario,
        r.matched_name
    FROM   source_extract s
    LEFT JOIN ranked_matches r
           ON s.source_id = r.source_id
          AND s.p_words IS NOT DISTINCT FROM r.p_words
          AND r.rn = 1
    """

    # 6. Two-phase UPDATE
    #
    # Phase 1: run the full matching logic as a plain SELECT into a temp table.
    #   DuckDB can fully optimize and parallelize a SELECT; no write overhead.
    #
    # Phase 2: trivial UPDATE from the temp table -- simple hash join, no
    #   complex subquery in the planner. Far faster than UPDATE...FROM(SELECT...).
    _TEMP = "__prontuario_matches"

    logger.info("[%s] Phase 1 -- running matching SELECT into temp table...", tag)
    t0 = time.perf_counter()
    source_con.execute(f"CREATE OR REPLACE TEMP TABLE {_TEMP} AS ({matching_query})")
    _t(f"[{tag}] Phase 1: SELECT into temp table", t0)

    n_matches = source_con.execute(f"SELECT COUNT(*) FROM {_TEMP}").fetchone()[0]
    logger.info("[%s] Temp table rows (candidates): %s", tag, n_matches)

    logger.info("[%s] Phase 2 -- applying UPDATE from temp table...", tag)
    t0 = time.perf_counter()
    source_con.execute(f"""
        UPDATE {source_schema}.\"{source_table}\" AS target
        SET
            prontuario    = m.prontuario,
            clinisys_name = m.matched_name
        FROM {_TEMP} m
        WHERE CAST(target.\"{id_col}\" AS VARCHAR) = m.source_id
    """)
    _t(f"[{tag}] Phase 2: UPDATE from temp table", t0)

    source_con.execute(f"DROP TABLE IF EXISTS {_TEMP}")
    logger.info("[%s] UPDATE complete.", tag)

    # 7. Stats
    t0 = time.perf_counter()
    row = source_con.execute(f"""
        SELECT
            COUNT(*)                                                  AS total,
            COUNT(CASE WHEN prontuario IS NOT NULL
                            AND prontuario != -1 THEN 1 END)         AS matched,
            COUNT(CASE WHEN prontuario IS NULL
                            OR  prontuario  = -1 THEN 1 END)         AS unmatched
        FROM {source_schema}."{source_table}"
    """).fetchone()
    _t(f"[{tag}] stats query", t0)

    total, matched, unmatched = row
    rate = matched / total if total else 0.0

    _t(f"[{tag}] TOTAL match_prontuario", t_total)
    logger.info(
        "[%s] Stats -- total: %s | matched: %s | unmatched: %s | rate: %.2f%%",
        tag, total, matched, unmatched, rate * 100,
    )

    return {
        "total":    total,
        "matched":  matched,
        "unmatched": unmatched,
        "rate":     rate,
        "groups":   [
            {
                "role":            g.role,
                "priority":        g.priority,
                "nome_col":        g.nome_col,
                "prontuario_cols": g.prontuario_cols,
            }
            for g in groups
        ],
    }


# ---------------------------------------------------------------------------
# Quick smoke-test  (python match_prontuario.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import sys
    import traceback
    from datetime import datetime

    # File lives in test_prontuario_matching/ — databases are one level up
    _HERE       = os.path.dirname(os.path.abspath(__file__))
    _LOG_DIR    = os.path.join(_HERE, "logs")
    os.makedirs(_LOG_DIR, exist_ok=True)

    _log_file   = os.path.join(_LOG_DIR, f"match_prontuario_{datetime.now():%Y%m%d_%H%M%S}.log")
    _formatter  = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    _fh = logging.FileHandler(_log_file, encoding="utf-8")
    _fh.setFormatter(_formatter)
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_formatter)

    logging.basicConfig(level=logging.INFO, handlers=[_sh, _fh])
    logger.info("Log file: %s", _log_file)

    DB_DIR      = os.path.join(_HERE, "..", "database")
    CLINISYS_DB = os.path.abspath(os.path.join(DB_DIR, "clinisys_all.duckdb"))
    LAKE_DB     = os.path.abspath(os.path.join(DB_DIR, "huntington_data_lake.duckdb"))

    if not os.path.exists(CLINISYS_DB):
        logger.error("clinisys_all.duckdb not found at %s — aborting.", CLINISYS_DB)
        sys.exit(1)

    try:
        con = duckdb.connect()  # in-memory — safe, does not touch production data

        con.execute(f"ATTACH '{LAKE_DB}' AS lake (READ_ONLY)")
        con.execute("""
            CREATE TABLE main.test_vendas AS
            SELECT * FROM lake.gold.protheus_mesclada_vendas
            LIMIT 500
        """)
        con.execute("DETACH lake")

        # Drop output columns so we exercise the column-creation path
        existing_cols = {r[1] for r in con.execute("PRAGMA table_info('main.test_vendas')").fetchall()}
        for col in ("prontuario", "clinisys_name"):
            if col in existing_cols:
                con.execute(f"ALTER TABLE main.test_vendas DROP COLUMN {col}")

        stats = match_prontuario(
            source_con    = con,
            db_path       = CLINISYS_DB,
            source_schema = "main",
            source_table  = "test_vendas",
            id_col        = "Paciente",
            name_col      = "Nom Paciente",
            label         = "smoke-test/mesclada_vendas",
        )

        print("\n=== Smoke-test stats ===")
        for k, v in stats.items():
            if k != "groups":
                print(f"  {k}: {v}")

        print("\n=== Discovered groups ===")
        for g in stats["groups"]:
            print(f"  [{g['role']}] nome={g['nome_col']}  prontuario_cols={g['prontuario_cols']}")

        sample = con.execute("""
            SELECT "Paciente", "Nom Paciente", prontuario, clinisys_name
            FROM   main.test_vendas
            WHERE  prontuario != -1
            LIMIT  10
        """).df()
        print("\nSample matched rows:")
        print(sample.to_string(index=False))

        con.close()

    except Exception:
        traceback.print_exc()
        sys.exit(1)
