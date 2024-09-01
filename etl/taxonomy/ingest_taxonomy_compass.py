#!/usr/bin/env python3
"""
EU Taxonomy Compass → PostgreSQL (multi-sheet + crosswalks).

- Ingests taxonomy.xlsx (all sheets) and ec_updated_mapping.xlsx (crosswalks).
- Normalizes columns (SC, DNSH, NACE, legal), composes IDs, and performs UPSERTs.
- DOES NOT delete existing data. Only INSERT with ON CONFLICT DO UPDATE/DO NOTHING.

Notes on idempotency and robustness:
- Objectives detection extended for 'waste' (CE) and 'nuclear'/'gas' (CCM) to avoid 'UNK'.
- activity_classification_map treats NULL code/label as empty strings to ensure UNIQUE works.
- SC/DNSH splitting is less aggressive (no commas), reducing over-segmentation of long texts.
- Skips sheets like 'Disclaimer'.
"""

from __future__ import annotations
import argparse
import hashlib
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import psycopg
from psycopg import Error as PsycopgError
from dotenv import load_dotenv

# ----------------------------- util & config ------------------------------


def setup_logging() -> None:
    """Configure simple console logging."""
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def sanitize_col(s: str) -> str:
    """Return a lowercase, underscore-separated version of a column name."""
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def as_opt_str(v: Any) -> Optional[str]:
    """Return a trimmed string or None for NA/empty-like inputs."""
    if (
        v is None
        or (isinstance(v, float) and pd.isna(v))
        or (isinstance(v, pd.Series) and v.empty)
    ):
        return None
    s = str(v).strip()
    return s or None


def split_many(val: Optional[str]) -> List[str]:
    """
    Generic splitter: split on semicolon, newline or comma.
    Use for fields like NACE, legal references, etc.
    """
    if val is None or pd.isna(val):
        return []
    s = str(val).strip()
    if not s:
        return []
    parts = re.split(r"[;\n,]", s)
    return [p.strip() for p in parts if p and p.strip()]


def split_criteria(val: Optional[str]) -> List[str]:
    """
    Criteria splitter: split only on semicolon or newline (NO commas).
    Use for SC/DNSH to avoid over-segmentation of descriptive texts.
    """
    if val is None or pd.isna(val):
        return []
    s = str(val).strip()
    if not s:
        return []
    parts = re.split(r"[;\n]", s)
    return [p.strip() for p in parts if p and p.strip()]


def norm_activity_number(v: Any) -> Optional[str]:
    """
    Normalize activity number:
    - 1.0 -> '1'
    - 1.10 -> '1.1'
    - Preserve non-numeric tokens as-is.
    """
    s = as_opt_str(v)
    if s is None:
        return None
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        s2 = f"{f:.6f}".rstrip("0").rstrip(".")
        return s2
    except Exception:
        return s


def hex_md5(s: str) -> str:
    """Return hex MD5 of a string (used for hash-based uniqueness)."""
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# Objective detection rules
OBJECTIVE_RULES: List[Tuple[str, str, str]] = [
    ("mitigation", "CCM", "Climate change mitigation"),
    ("adaptation", "CCA", "Climate change adaptation"),
    ("water", "WAT", "Sustainable use and protection of water and marine resources"),
    ("circular", "CE", "Transition to a circular economy"),
    ("waste", "CE", "Transition to a circular economy"),  # extra
    ("pollution", "POL", "Pollution prevention and control"),
    ("biod", "BIO", "Protection and restoration of biodiversity and ecosystems"),
    ("nuclear", "CCM", "Climate change mitigation"),  # extra
    ("gas", "CCM", "Climate change mitigation"),  # extra
]


def detect_objective(sheet_name: str) -> Tuple[str, str]:
    """Guess objective code/name from the sheet name using OBJECTIVE_RULES."""
    s = sheet_name.strip().lower()
    for key, code, fullname in OBJECTIVE_RULES:
        if key in s:
            return code, fullname
    # Fallback: pass sheet name as objective name with code 'UNK'
    return "UNK", sheet_name.strip()


def should_skip_sheet(sheet_name: str) -> bool:
    """Return True for helper/disclaimer sheets that should be ignored."""
    s = sanitize_col(sheet_name)
    return "disclaimer" in s or s in {"notes", "note", "readme"}


# Possible families in crosswalk
FAMILY_HINTS: Dict[str, List[str]] = {
    "FTSE": ["ftse", "icb", "grcs"],
    "TRBC": ["trbc"],
    "BICS": ["bics", "bloomberg"],
    "RBICS": ["rbics", "factset"],
    "MSCI": ["msci"],
    "Refinitiv": ["refinitiv"],
    "GICS": ["gics", "s&p", "sp"],  # S&P GICS
}

# ----------------------------- DB schema DDL ------------------------------


DDL: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS release (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        source_url TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS taxonomy_objective (
        id SERIAL PRIMARY KEY,
        code TEXT NOT NULL,
        name TEXT,
        release_id INTEGER NOT NULL REFERENCES release(id),
        UNIQUE (release_id, code)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS taxonomy_activity (
        id SERIAL PRIMARY KEY,
        compass_id TEXT NOT NULL,
        title TEXT NOT NULL,
        objective_id INTEGER NOT NULL REFERENCES taxonomy_objective(id),
        description TEXT,
        release_id INTEGER NOT NULL REFERENCES release(id),
        UNIQUE (release_id, compass_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS activity_nace (
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        nace_code TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (activity_id, nace_code)
    );
    """,
    # ------- hash-based tables to avoid huge index entries on long TEXT -------
    """
    CREATE TABLE IF NOT EXISTS criteria_sc (
        id SERIAL PRIMARY KEY,
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        criterion_text TEXT NOT NULL,
        criterion_hash TEXT NOT NULL,
        UNIQUE (activity_id, criterion_hash)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS criteria_dnsh (
        id SERIAL PRIMARY KEY,
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        criterion_text TEXT NOT NULL,
        criterion_hash TEXT NOT NULL,
        UNIQUE (activity_id, criterion_hash)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS criteria_ms (
        id SERIAL PRIMARY KEY,
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        reference TEXT NOT NULL,
        reference_hash TEXT NOT NULL,
        UNIQUE (activity_id, reference_hash)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS legal_reference (
        id SERIAL PRIMARY KEY,
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        act_name TEXT,
        article TEXT,
        annex TEXT,
        url TEXT,
        legal_hash TEXT NOT NULL,
        UNIQUE (activity_id, legal_hash)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS activity_classification_map (
        activity_id INTEGER NOT NULL REFERENCES taxonomy_activity(id) ON DELETE CASCADE,
        source TEXT NOT NULL,      -- FTSE/TRBC/BICS/RBICS/MSCI/Refinitiv/GICS
        code   TEXT,               -- may be NULL in source data
        label  TEXT,
        UNIQUE (activity_id, source, code, label)
    );
    """,
]


def ensure_schema(cur) -> None:
    """Create all tables if they do not exist."""
    for sql in DDL:
        cur.execute(sql)


# ----------------------------- DB helpers --------------------------------


def exec_scalar(cur, sql: str, params: dict) -> int:
    """Execute a statement expected to return a single integer (e.g., an id)."""
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Query did not return any rows.")
    return int(row[0])


def upsert_release(cur, name: str, source_url: str) -> int:
    """Insert or update a release record and return its id."""
    sql = """
        INSERT INTO release (name, source_url)
        VALUES (%(name)s, %(source_url)s)
        ON CONFLICT (name) DO UPDATE SET source_url = EXCLUDED.source_url
        RETURNING id;
    """
    return exec_scalar(cur, sql, {"name": name, "source_url": source_url})


def upsert_objective(cur, release_id: int, code: str, name: Optional[str]) -> int:
    """Insert or update a taxonomy objective and return its id."""
    sql = """
        INSERT INTO taxonomy_objective (code, name, release_id)
        VALUES (%(code)s, %(name)s, %(release_id)s)
        ON CONFLICT (release_id, code)
        DO UPDATE SET name = COALESCE(EXCLUDED.name, taxonomy_objective.name)
        RETURNING id;
    """
    return exec_scalar(cur, sql, {"code": code, "name": name, "release_id": release_id})


def upsert_activity(
    cur,
    release_id: int,
    objective_id: int,
    compass_id: str,
    title: str,
    description: Optional[str],
) -> int:
    """Insert or update a taxonomy activity and return its id."""
    sql = """
        INSERT INTO taxonomy_activity (compass_id, title, objective_id, description, release_id)
        VALUES (%(compass_id)s, %(title)s, %(objective_id)s, %(description)s, %(release_id)s)
        ON CONFLICT (release_id, compass_id)
        DO UPDATE SET title = EXCLUDED.title,
                      objective_id = EXCLUDED.objective_id,
                      description = EXCLUDED.description
        RETURNING id;
    """
    return exec_scalar(
        cur,
        sql,
        {
            "compass_id": compass_id,
            "title": title,
            "objective_id": objective_id,
            "description": description,
            "release_id": release_id,
        },
    )


def insert_activity_nace(
    cur, activity_id: int, nace_code: str, note: Optional[str] = None
) -> None:
    """Insert a NACE code for an activity (idempotent)."""
    sql = """
        INSERT INTO activity_nace (activity_id, nace_code, note)
        VALUES (%(activity_id)s, %(nace_code)s, %(note)s)
        ON CONFLICT (activity_id, nace_code) DO NOTHING;
    """
    cur.execute(sql, {"activity_id": activity_id, "nace_code": nace_code, "note": note})


def insert_sc(cur, activity_id: int, criterion_text: str) -> None:
    """Insert a Substantial Contribution criterion (idempotent, hash-based)."""
    h = hex_md5(criterion_text)
    sql = """
        INSERT INTO criteria_sc (activity_id, criterion_text, criterion_hash)
        VALUES (%(activity_id)s, %(criterion_text)s, %(criterion_hash)s)
        ON CONFLICT (activity_id, criterion_hash) DO NOTHING;
    """
    cur.execute(
        sql,
        {
            "activity_id": activity_id,
            "criterion_text": criterion_text,
            "criterion_hash": h,
        },
    )


def insert_dnsh(cur, activity_id: int, criterion_text: str) -> None:
    """Insert a DNSH criterion (idempotent, hash-based)."""
    h = hex_md5(criterion_text)
    sql = """
        INSERT INTO criteria_dnsh (activity_id, criterion_text, criterion_hash)
        VALUES (%(activity_id)s, %(criterion_text)s, %(criterion_hash)s)
        ON CONFLICT (activity_id, criterion_hash) DO NOTHING;
    """
    cur.execute(
        sql,
        {
            "activity_id": activity_id,
            "criterion_text": criterion_text,
            "criterion_hash": h,
        },
    )


def insert_ms(cur, activity_id: int, reference: str) -> None:
    """Insert a minimum safeguards reference (idempotent, hash-based)."""
    h = hex_md5(reference)
    sql = """
        INSERT INTO criteria_ms (activity_id, reference, reference_hash)
        VALUES (%(activity_id)s, %(reference)s, %(reference_hash)s)
        ON CONFLICT (activity_id, reference_hash) DO NOTHING;
    """
    cur.execute(
        sql, {"activity_id": activity_id, "reference": reference, "reference_hash": h}
    )


def insert_legal(
    cur,
    activity_id: int,
    act_name: Optional[str],
    article: Optional[str],
    annex: Optional[str],
    url: Optional[str],
) -> None:
    """Insert a legal reference tuple (idempotent, hash-based)."""
    a = act_name or ""
    b = article or ""
    c = annex or ""
    d = url or ""
    h = hex_md5("|".join([a, b, c, d]))
    sql = """
        INSERT INTO legal_reference (activity_id, act_name, article, annex, url, legal_hash)
        VALUES (%(activity_id)s, %(act_name)s, %(article)s, %(annex)s, %(url)s, %(legal_hash)s)
        ON CONFLICT (activity_id, legal_hash) DO NOTHING;
    """
    cur.execute(
        sql,
        {
            "activity_id": activity_id,
            "act_name": act_name,
            "article": article,
            "annex": annex,
            "url": url,
            "legal_hash": h,
        },
    )


def insert_classification_map(
    cur, activity_id: int, source: str, code: Optional[str], label: Optional[str]
) -> None:
    """
    Insert a classification mapping (idempotent).
    To ensure uniqueness across NULLs, we coalesce to empty strings before insert.
    """
    code = code or ""
    label = label or ""
    sql = """
        INSERT INTO activity_classification_map (activity_id, source, code, label)
        VALUES (%(activity_id)s, %(source)s, %(code)s, %(label)s)
        ON CONFLICT (activity_id, source, code, label) DO NOTHING;
    """
    cur.execute(
        sql,
        {"activity_id": activity_id, "source": source, "code": code, "label": label},
    )


# ----------------------------- readers -----------------------------------


def read_all_sheets(path: Path) -> Dict[str, pd.DataFrame]:
    """Read every sheet of an Excel file into dataframes (dtype=object), preserving None for NA."""
    xls = pd.ExcelFile(path)
    out: Dict[str, pd.DataFrame] = {}
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, dtype=object)
        out[sh] = df.where(df.notna(), None)
    return out


def extract_first(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Return the first existing column (case-insensitive) among 'candidates', or None if absent."""
    cols = {sanitize_col(c): c for c in df.columns}
    for cand in candidates:
        key = sanitize_col(cand)
        if key in cols:
            return cols[key]
    return None


def normalize_taxonomy_sheet(df: pd.DataFrame, sheet_name: str) -> List[Dict[str, Any]]:
    """
    Convert a taxonomy.xlsx sheet into normalized records.

    Expected keys per record:
    - objective_code, objective_name
    - activity_compass_id, activity_title, activity_description
    - nace_codes, sc_criteria, dnsh_criteria, ms_references
    - legal_act, legal_article, legal_annex, legal_url
    """
    obj_code, obj_name = detect_objective(sheet_name)

    col_actnum = extract_first(
        df, ["Activity number", "Activity No", "Activity ID", "Activity_number"]
    )
    col_title = extract_first(df, ["Activity", "Activity title"])
    col_desc = extract_first(df, ["Description", "Activity description"])
    col_sc = extract_first(
        df, ["Substantial contribution criteria", "SC", "SC criteria"]
    )

    # NACE (many variants)
    col_nace = next((c for c in df.columns if "nace" in c.lower()), None)

    # DNSH: any column whose sanitized name starts with 'dnsh'
    dnsh_cols = [c for c in df.columns if sanitize_col(c).startswith("dnsh")]

    # Legal / MS (if present)
    col_lact = extract_first(df, ["Legal act", "Legal_act"])
    col_lart = extract_first(df, ["Legal article", "Article"])
    col_lann = extract_first(df, ["Legal annex", "Annex"])
    col_lurl = extract_first(df, ["Legal URL", "URL"])
    col_ms = extract_first(df, ["MS references", "Minimum safeguards", "MS"])

    records: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        actnum = norm_activity_number(r.get(col_actnum)) if col_actnum else None
        title = as_opt_str(r.get(col_title)) if col_title else None
        if not actnum or not title:
            continue  # invalid row

        compass_id = f"{obj_code} {actnum}"

        # SC: keep as text; split later with split_criteria
        sc_text = as_opt_str(r.get(col_sc)) if col_sc else None

        # DNSH: concatenate all DNSH columns (then split with split_criteria)
        dnsh_parts: List[str] = []
        for c in dnsh_cols:
            v = as_opt_str(r.get(c))
            if v:
                dnsh_parts.append(v)
        dnsh_text = "; ".join(dnsh_parts) if dnsh_parts else None

        # NACE: supports multiple separated by ';' or ','
        nace_text = as_opt_str(r.get(col_nace)) if col_nace else None

        rec = {
            "objective_code": obj_code,
            "objective_name": obj_name,
            "activity_compass_id": compass_id,
            "activity_title": title,
            "activity_description": as_opt_str(r.get(col_desc)) if col_desc else None,
            "nace_codes": nace_text,
            "sc_criteria": sc_text,
            "dnsh_criteria": dnsh_text,
            "ms_references": as_opt_str(r.get(col_ms)) if col_ms else None,
            "legal_act": as_opt_str(r.get(col_lact)) if col_lact else None,
            "legal_article": as_opt_str(r.get(col_lart)) if col_lart else None,
            "legal_annex": as_opt_str(r.get(col_lann)) if col_lann else None,
            "legal_url": as_opt_str(r.get(col_lurl)) if col_lurl else None,
        }
        records.append(rec)

    return records


def parse_taxonomy_xlsx(path: Path) -> List[Dict[str, Any]]:
    """Parse taxonomy.xlsx across all sheets, skipping empty or disclaimer-like sheets."""
    sheets = read_all_sheets(path)
    allrecs: List[Dict[str, Any]] = []
    for sh, df in sheets.items():
        if should_skip_sheet(sh):
            logging.debug("Skipping sheet: %s", sh)
            continue
        if df is None or df.empty:
            continue
        allrecs.extend(normalize_taxonomy_sheet(df, sh))
    logging.info("taxonomy.xlsx: normalized activities = %d", len(allrecs))
    return allrecs


def family_for_column(colname: str) -> Optional[str]:
    """Infer a classification family from a column name."""
    s = colname.lower()
    for fam, hints in FAMILY_HINTS.items():
        for h in hints:
            if h in s:
                return fam
    return None


def parse_crosswalk_xlsx(path: Path) -> List[Dict[str, Any]]:
    """
    Parse ec_updated_mapping.xlsx into per-activity classification mappings.

    Each row may yield multiple records (one per detected classification family).
    """
    sheets = read_all_sheets(path)
    out: List[Dict[str, Any]] = []

    for sh, df in sheets.items():
        if should_skip_sheet(sh):
            logging.debug("Skipping sheet: %s", sh)
            continue
        if df is None or df.empty:
            continue

        obj_code, obj_name = detect_objective(sh)
        col_actnum = extract_first(
            df, ["Activity number", "Activity No", "Activity ID", "Activity_number"]
        )
        col_title = extract_first(df, ["Activity", "Activity title"])
        col_nace = next((c for c in df.columns if "nace" in c.lower()), None)

        # Group columns by tentative family and by "code"/"label" role
        fam_cols: Dict[str, Dict[str, List[str]]] = {}
        for c in df.columns:
            fam = family_for_column(c)
            if not fam:
                continue
            key = (
                "code" if any(k in sanitize_col(c) for k in ["code", "id"]) else "label"
            )
            fam_cols.setdefault(fam, {}).setdefault(key, []).append(c)

        for _, r in df.iterrows():
            actnum = norm_activity_number(r.get(col_actnum)) if col_actnum else None
            title = as_opt_str(r.get(col_title)) if col_title else None
            if not actnum or not title:
                continue

            compass_id = f"{obj_code} {actnum}"
            nace_text = as_opt_str(r.get(col_nace)) if col_nace else None

            # For each family, emit (code, label) pairs
            for fam, kinds in fam_cols.items():
                codes = kinds.get("code", []) or [None]
                labels = kinds.get("label", []) or [None]

                seen_pairs = set()
                for ccol in codes:
                    code_val = as_opt_str(r.get(ccol)) if ccol else None
                    for lcol in labels:
                        label_val = as_opt_str(r.get(lcol)) if lcol else None
                        if not code_val and not label_val:
                            continue
                        key = (code_val or "", label_val or "")
                        if key in seen_pairs:
                            continue
                        seen_pairs.add(key)
                        out.append(
                            {
                                "objective_code": obj_code,
                                "objective_name": obj_name,
                                "activity_compass_id": compass_id,
                                "activity_title": title,
                                "nace_codes": nace_text,
                                "family": fam,
                                "code": code_val,
                                "label": label_val,
                            }
                        )

    logging.info("ec_updated_mapping.xlsx: normalized mappings = %d", len(out))
    return out


# ----------------------------- CLI args ----------------------------------


@dataclass
class Args:
    taxonomy_xlsx: Optional[Path]
    mapping_xlsx: Optional[Path]
    database_url: str
    release_name: str
    source_url: str


def parse_args() -> Args:
    """Parse CLI arguments and basic file existence checks."""
    ap = argparse.ArgumentParser(
        description="Ingest EU Taxonomy & Crosswalks into PostgreSQL"
    )
    ap.add_argument("--taxonomy-xlsx", help="Path to taxonomy.xlsx", required=False)
    ap.add_argument(
        "--mapping-xlsx", help="Path to ec_updated_mapping.xlsx", required=False
    )
    ap.add_argument(
        "--database-url", required=True, help="postgresql://user:pass@host:port/db"
    )
    ap.add_argument(
        "--release-name", required=True, help="Unique release name (for versioning)"
    )
    ap.add_argument("--source-url", required=True, help="Data source URL/label")
    ns = ap.parse_args()

    if not ns.taxonomy_xlsx and not ns.mapping_xlsx:
        ap.error("You must provide --taxonomy-xlsx and/or --mapping-xlsx")

    tx = Path(ns.taxonomy_xlsx).resolve() if ns.taxonomy_xlsx else None
    mx = Path(ns.mapping_xlsx).resolve() if ns.mapping_xlsx else None
    if tx and not tx.exists():
        ap.error(f"File not found: {tx}")
    if mx and not mx.exists():
        ap.error(f"File not found: {mx}")

    return Args(
        taxonomy_xlsx=tx,
        mapping_xlsx=mx,
        database_url=str(ns.database_url),
        release_name=str(ns.release_name),
        source_url=str(ns.source_url),
    )


# ----------------------------- main ingest -------------------------------


def ingest_taxonomy(cur, release_id: int, records: List[Dict[str, Any]]) -> None:
    """Ingest normalized taxonomy records into the database."""
    objective_ids: Dict[str, int] = {}
    for rec in records:
        oc = rec["objective_code"]
        oname = rec["objective_name"]
        if oc not in objective_ids:
            objective_ids[oc] = upsert_objective(cur, release_id, oc, oname)
        oid = objective_ids[oc]

        compass_id = rec["activity_compass_id"]
        title = rec["activity_title"]
        desc = rec.get("activity_description")
        aid = upsert_activity(cur, release_id, oid, compass_id, title, desc)

        # NACE
        for nc in split_many(rec.get("nace_codes")):
            insert_activity_nace(cur, aid, nc, None)

        # SC / DNSH / MS (hash-based)
        for s in split_criteria(rec.get("sc_criteria")):
            insert_sc(cur, aid, s)
        for d in split_criteria(rec.get("dnsh_criteria")):
            insert_dnsh(cur, aid, d)
        for m in split_many(rec.get("ms_references")):
            insert_ms(cur, aid, m)

        # Legal references: pairwise by position, padding with None
        acts = split_many(rec.get("legal_act"))
        arts = split_many(rec.get("legal_article"))
        anxs = split_many(rec.get("legal_annex"))
        urls = split_many(rec.get("legal_url"))
        maxlen = max([len(acts), len(arts), len(anxs), len(urls), 1])
        for i in range(maxlen):
            insert_legal(
                cur,
                aid,
                acts[i] if i < len(acts) else None,
                arts[i] if i < len(arts) else None,
                anxs[i] if i < len(anxs) else None,
                urls[i] if i < len(urls) else None,
            )


def ingest_crosswalk(cur, release_id: int, rows: List[Dict[str, Any]]) -> None:
    """Ingest crosswalk mapping rows into the database."""
    objective_ids: Dict[str, int] = {}
    activity_ids: Dict[
        Tuple[str, str], int
    ] = {}  # (objective_code, compass_id) -> taxonomy_activity.id

    for rec in rows:
        oc = rec["objective_code"]
        oname = rec["objective_name"]
        if oc not in objective_ids:
            objective_ids[oc] = upsert_objective(cur, release_id, oc, oname)
        oid = objective_ids[oc]

        compass_id = rec["activity_compass_id"]
        title = rec["activity_title"]

        key = (oc, compass_id)
        if key not in activity_ids:
            aid = upsert_activity(cur, release_id, oid, compass_id, title, None)
            activity_ids[key] = aid
        aid = activity_ids[key]

        # NACE from crosswalk (if any)
        for nc in split_many(rec.get("nace_codes")):
            insert_activity_nace(cur, aid, nc, None)

        insert_classification_map(
            cur, aid, rec["family"], rec.get("code"), rec.get("label")
        )


def main() -> int:
    """Entry-point: parse inputs, read Excel files, and ingest into PostgreSQL."""
    setup_logging()
    load_dotenv()
    args = parse_args()

    # Parse inputs
    tax_records: List[Dict[str, Any]] = []
    if args.taxonomy_xlsx:
        logging.info("Reading taxonomy.xlsx: %s", args.taxonomy_xlsx)
        tax_records = parse_taxonomy_xlsx(args.taxonomy_xlsx)

    cross_rows: List[Dict[str, Any]] = []
    if args.mapping_xlsx:
        logging.info("Reading ec_updated_mapping.xlsx: %s", args.mapping_xlsx)
        cross_rows = parse_crosswalk_xlsx(args.mapping_xlsx)

    # Database ingest
    try:
        with psycopg.connect(args.database_url) as conn:
            with conn.cursor() as cur:
                ensure_schema(cur)
                release_id = upsert_release(cur, args.release_name, args.source_url)
                logging.info("Release id=%s", release_id)

                if tax_records:
                    ingest_taxonomy(cur, release_id, tax_records)
                    logging.info("Taxonomy: processed %d activities", len(tax_records))
                if cross_rows:
                    ingest_crosswalk(cur, release_id, cross_rows)
                    logging.info("Crosswalks: processed %d rows", len(cross_rows))

        logging.info("Ingestion completed successfully.")
        return 0

    except PsycopgError as db_exc:
        logging.critical("Database error: %s", db_exc)
        return 1
    except Exception as exc:
        logging.critical("Unexpected error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
