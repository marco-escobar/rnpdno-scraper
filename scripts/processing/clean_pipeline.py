"""RNPDNO Cleaning Pipeline
=========================
Produces four cleaned, schema-normalised CSV files from the consolidated
raw RNPDNO Parquet exports.  Idempotent: re-run after updating
``data/manual/geo_overrides.csv`` to improve geo-join coverage.

Usage
-----
    python scripts/processing/clean_pipeline.py

Inputs
------
data/raw/rnpdno/consolidated/
    rnpdno_total_2015_2025.parquet
    rnpdno_desaparecidas_no_localizadas_2015_2025.parquet
    rnpdno_localizadas_con_vida_2015_2025.parquet
    rnpdno_localizadas_sin_vida_2015_2025.parquet

data/external/
    ageeml_catalog.csv          – INEGI municipal geo-code catalog (ISO-8859-1)

data/manual/
    geo_overrides.csv           – manual municipality name overrides
    geo_state_corrections.csv   – B1 unique-nationwide state corrections

Outputs
-------
data/processed/
    rnpdno_total.csv
    rnpdno_disappeared_not_located.csv
    rnpdno_located_alive.csv
    rnpdno_located_dead.csv

logs/
    pipeline.log
    quality_report.csv
"""

import logging
import sys
from collections import Counter
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from normalize import normalize_text, normalize_text_aggressive  # noqa: E402

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "rnpdno" / "consolidated"
EXTERNAL_DIR = PROJECT_ROOT / "data" / "external"
MANUAL_DIR = PROJECT_ROOT / "data" / "manual"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
LOG_DIR = PROJECT_ROOT / "logs"

DATASETS = {
    "rnpdno_total": {
        "raw_file": "rnpdno_total_2015_2025.parquet",
        "output_file": "rnpdno_total.csv",
    },
    "rnpdno_disappeared_not_located": {
        "raw_file": "rnpdno_desaparecidas_no_localizadas_2015_2025.parquet",
        "output_file": "rnpdno_disappeared_not_located.csv",
    },
    "rnpdno_located_alive": {
        "raw_file": "rnpdno_localizadas_con_vida_2015_2025.parquet",
        "output_file": "rnpdno_located_alive.csv",
    },
    "rnpdno_located_dead": {
        "raw_file": "rnpdno_localizadas_sin_vida_2015_2025.parquet",
        "output_file": "rnpdno_located_dead.csv",
    },
}

TARGET_SCHEMA = [
    "cvegeo", "cve_estado", "state", "cve_mun", "municipality",
    "male", "female", "undefined", "total",
    "year", "month", "status_id",
]

# geo columns added during the join stage
_GEO_COLS = [
    "geo_cvegeo", "geo_cve_estado", "geo_cve_mun",
    "geo_state", "geo_municipality",
]

# Sentinel municipality labels -> cve_mun
_SENTINEL_MUNI = {
    "sin municipio de referencia": 998,
    "se desconoce": 999,
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "pipeline.log", mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------------------------------------------------------------------------
# Reference table (AGEEML)
# ---------------------------------------------------------------------------
def build_reference() -> pl.DataFrame:
    """Load AGEEML catalog, normalize names, return reference DataFrame."""
    logger = logging.getLogger(__name__)
    ageeml_path = EXTERNAL_DIR / "ageeml_catalog.csv"
    logger.info("Loading AGEEML from %s", ageeml_path)

    df = pl.read_csv(
        ageeml_path, encoding="latin1",
        infer_schema_length=10000,
        null_values=["----", "---", "--", "-"],
    )
    logger.info("AGEEML raw: %d rows", len(df))

    # zero-pad codes
    df = df.with_columns([
        pl.col("CVEGEO").cast(pl.Utf8).str.strip_chars().str.zfill(5).alias("cvegeo"),
        pl.col("CVE_ENT").cast(pl.Utf8).str.strip_chars().str.zfill(2).alias("cve_estado"),
        pl.col("CVE_MUN").cast(pl.Utf8).str.strip_chars().str.zfill(3).alias("cve_mun"),
    ])

    # normalized name columns (applied via map_elements since normalize_text is Python)
    df = df.with_columns([
        pl.col("NOM_ENT").map_elements(normalize_text, return_dtype=pl.Utf8).alias("state"),
        pl.col("NOM_MUN").map_elements(normalize_text, return_dtype=pl.Utf8).alias("municipality"),
        pl.col("NOM_MUN").map_elements(normalize_text_aggressive, return_dtype=pl.Utf8).alias("municipality_agg"),
    ])

    logger.info("AGEEML reference: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# 1. Load & normalise raw data
# ---------------------------------------------------------------------------
def load_and_normalize(raw_path: Path, dataset_name: str) -> pl.DataFrame:
    """Read one raw Parquet, add normalised text columns and a 2-digit state code."""
    logger = logging.getLogger(__name__)
    logger.info("[%s] Reading %s", dataset_name, raw_path.name)

    df = pl.read_parquet(raw_path)

    # text normalization
    df = df.with_columns([
        pl.col("municipio").map_elements(normalize_text, return_dtype=pl.Utf8).alias("municipio_norm"),
        pl.col("municipio").map_elements(normalize_text_aggressive, return_dtype=pl.Utf8).alias("municipio_norm_agg"),
        pl.col("estado").map_elements(normalize_text, return_dtype=pl.Utf8).alias("estado_norm"),
    ])

    # 2-digit state code; null for id_estado == 33
    df = df.with_columns(
        pl.when(pl.col("id_estado") == 33)
        .then(pl.lit(None))
        .otherwise(pl.col("id_estado").cast(pl.Utf8).str.zfill(2))
        .alias("cve_estado_raw")
    )

    # Deduplicate re-scrape duplicates: when the same source record was
    # scraped twice, the later row (further down in the consolidated
    # Parquet) carries the updated counts.  Keep last occurrence.
    raw_key = ["id_estado", "municipio", "anio", "mes", "id_estatus_victima"]
    n_before = len(df)
    df = df.unique(subset=raw_key, keep="last")
    n_deduped = n_before - len(df)
    if n_deduped:
        logger.info("[%s] Raw dedup: removed %d re-scrape duplicate(s), kept last", dataset_name, n_deduped)

    logger.info("[%s] %d rows loaded & normalised", dataset_name, len(df))
    return df


# ---------------------------------------------------------------------------
# 2. Discrepancy detection
# ---------------------------------------------------------------------------
def detect_discrepancies(
    df: pl.DataFrame, ref: pl.DataFrame, dataset_name: str
) -> list[dict]:
    """Flag anomalies: id_estado==33 and possible wrong-state records."""
    logger = logging.getLogger(__name__)

    valid_pairs = set(
        zip(ref["cve_estado"].to_list(), ref["municipality"].to_list())
    )
    muni_states: dict[str, set] = {}
    for m, s in zip(ref["municipality"].to_list(), ref["cve_estado"].to_list()):
        muni_states.setdefault(m, set()).add(s)

    rows = []
    df_pd = df.select(
        "id_estado", "municipio", "municipio_norm", "cve_estado_raw",
        "estado", "total_personas"
    ).to_pandas()

    # id_estado == 33
    unk = df_pd[df_pd["id_estado"] == 33]
    for (id_est, muni_raw, muni_norm), grp in unk.groupby(
        ["id_estado", "municipio", "municipio_norm"]
    ):
        if muni_norm in muni_states:
            rows.append(dict(
                dataset=dataset_name, id_estado=id_est,
                municipio_raw=muni_raw, municipio_norm=muni_norm,
                row_count=len(grp),
                total_persons=int(grp["total_personas"].sum()),
                issue_type="possible_wrong_state",
                notes=f"id_estado=33 but '{muni_raw}' exists under {sorted(muni_states[muni_norm])}",
            ))
        else:
            rows.append(dict(
                dataset=dataset_name, id_estado=id_est,
                municipio_raw=muni_raw, municipio_norm=muni_norm,
                row_count=len(grp),
                total_persons=int(grp["total_personas"].sum()),
                issue_type="unknown_state",
                notes="id_estado=33; municipio not in AGEEML",
            ))

    # known state but municipality under different state
    known = df_pd[df_pd["id_estado"] != 33]
    for (id_est, est_raw, muni_raw, muni_norm), grp in known.groupby(
        ["id_estado", "cve_estado_raw", "municipio", "municipio_norm"]
    ):
        if (est_raw, muni_norm) not in valid_pairs and muni_norm in muni_states:
            rows.append(dict(
                dataset=dataset_name, id_estado=id_est,
                municipio_raw=muni_raw, municipio_norm=muni_norm,
                row_count=len(grp),
                total_persons=int(grp["total_personas"].sum()),
                issue_type="possible_wrong_state",
                notes=f"Not under estado {est_raw}; found under {sorted(muni_states[muni_norm])}",
            ))

    if rows:
        logger.info("[%s] Discrepancies: %d groups flagged", dataset_name, len(rows))
    return rows


# ---------------------------------------------------------------------------
# 3. Two-pass geo-join
# ---------------------------------------------------------------------------
def _build_lookups(ref: pl.DataFrame):
    """Return (simple_map, agg_map) dicts for geo-join."""
    ref_pd = ref.select(
        "cvegeo", "cve_estado", "cve_mun", "state", "municipality", "municipality_agg"
    ).to_pandas()

    def _row_to_geo(r):
        return dict(
            geo_cvegeo=r["cvegeo"],
            geo_cve_estado=r["cve_estado"],
            geo_cve_mun=r["cve_mun"],
            geo_state=r["state"],
            geo_municipality=r["municipality"],
        )

    simple_map = {}
    for _, r in ref_pd.iterrows():
        simple_map.setdefault((r["cve_estado"], r["municipality"]), _row_to_geo(r))

    # aggressive: only unambiguous keys
    agg_counts = ref_pd.groupby(["cve_estado", "municipality_agg"]).size()
    unique_agg_keys = set(agg_counts[agg_counts == 1].index)

    agg_map = {}
    for _, r in ref_pd.iterrows():
        key = (r["cve_estado"], r["municipality_agg"])
        if key in unique_agg_keys:
            agg_map[key] = _row_to_geo(r)

    return simple_map, agg_map


def geo_join(df: pl.DataFrame, ref: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Two-pass geo-join: standard then aggressive normalization."""
    logger = logging.getLogger(__name__)

    simple_map, agg_map = _build_lookups(ref)

    # Convert to pandas for row-level dict lookups (performance acceptable for ~60k rows)
    pdf = df.to_pandas()
    import pandas as pd

    est_filled = pdf["cve_estado_raw"].fillna("__NA__")
    keys_simple = list(zip(est_filled, pdf["municipio_norm"]))
    keys_agg = list(zip(est_filled, pdf["municipio_norm_agg"]))

    matched_simple = pd.Series(keys_simple, index=pdf.index).map(simple_map)
    matched_agg = pd.Series(keys_agg, index=pdf.index).map(agg_map)

    for col in _GEO_COLS:
        simple_vals = matched_simple.apply(
            lambda x, c=col: x[c] if isinstance(x, dict) else pd.NA
        )
        agg_vals = matched_agg.apply(
            lambda x, c=col: x[c] if isinstance(x, dict) else pd.NA
        )
        pdf[col] = simple_vals.where(simple_vals.notna(), agg_vals)

    is_simple = matched_simple.apply(lambda x: isinstance(x, dict))
    is_agg = matched_agg.apply(lambda x: isinstance(x, dict))
    is_unknown = pdf["cve_estado_raw"].isna()

    pdf["match_type"] = "unmatched"
    pdf.loc[is_unknown, "match_type"] = "unknown_state"
    pdf.loc[is_simple & ~is_unknown, "match_type"] = "simple"
    pdf.loc[~is_simple & is_agg & ~is_unknown, "match_type"] = "aggressive"

    counts = Counter(pdf["match_type"])
    logger.info("[%s] Geo-join: %s", dataset_name, dict(counts))

    return pl.from_pandas(pdf)


# ---------------------------------------------------------------------------
# 4a. Apply B1 state corrections (pre-join)
# ---------------------------------------------------------------------------
def load_state_corrections() -> dict:
    """Read geo_state_corrections.csv; return lookup dict.

    Schema: source_id_estado, raw_municipio, target_cve_estado, target_cve_mun
    Key: (source_id_estado zero-padded, normalize_text(raw_municipio))
    Value: target_cve_estado (zero-padded 2-digit)
    """
    logger = logging.getLogger(__name__)
    path = MANUAL_DIR / "geo_state_corrections.csv"
    if not path.exists() or path.stat().st_size < 10:
        logger.info("No geo_state_corrections.csv – skipping state corrections")
        return {}

    import pandas as pd
    corr = pd.read_csv(path)
    corr["source_id_estado"] = corr["source_id_estado"].astype(str).str.zfill(2)
    corr["target_cve_estado"] = corr["target_cve_estado"].astype(str).str.zfill(2)
    corr["_key_muni"] = corr["raw_municipio"].apply(normalize_text)

    lookup = dict(zip(
        zip(corr["source_id_estado"], corr["_key_muni"]),
        corr["target_cve_estado"],
    ))
    logger.info("Loaded %d state correction(s)", len(lookup))
    return lookup


def apply_state_corrections(
    df: pl.DataFrame, corrections: dict, dataset_name: str
) -> pl.DataFrame:
    """Re-assign cve_estado_raw for B1 (unique-nationwide) state errors.

    Must run BEFORE geo_join so the corrected state key is used in AGEEML lookup.
    """
    logger = logging.getLogger(__name__)
    if not corrections:
        return df

    pdf = df.to_pandas()
    import pandas as pd

    keys = list(zip(pdf["cve_estado_raw"].fillna(""), pdf["municipio_norm"]))
    new_est = pd.Series(keys, index=pdf.index).map(corrections)

    mask = new_est.notna()
    n = int(mask.sum())
    if n:
        pdf.loc[mask, "cve_estado_raw"] = new_est[mask]
        logger.info("[%s] State corrections applied: %d row(s) re-assigned", dataset_name, n)

    return pl.from_pandas(pdf)


# ---------------------------------------------------------------------------
# 4b. Apply manual overrides
# ---------------------------------------------------------------------------
def load_overrides(ref: pl.DataFrame) -> dict:
    """Read geo_overrides.csv; return lookup dict.

    Schema: raw_id_estado, raw_municipio, cvegeo, notes, source_dataset
    Key: (raw_id_estado zero-padded, normalize_text(raw_municipio))
    Value: dict with geo fields (looked up from AGEEML ref)
    """
    logger = logging.getLogger(__name__)
    path = MANUAL_DIR / "geo_overrides.csv"
    if not path.exists() or path.stat().st_size < 10:
        logger.info("No geo_overrides.csv – skipping overrides")
        return {}

    import pandas as pd
    ov = pd.read_csv(path, dtype=str)

    # Build AGEEML lookup by cvegeo
    ref_pd = ref.select("cvegeo", "cve_estado", "cve_mun", "state", "municipality").to_pandas()
    ref_idx = ref_pd.set_index("cvegeo")

    override_map = {}
    for _, row in ov.iterrows():
        raw_id_estado = str(row["raw_id_estado"]).strip().zfill(2)
        raw_mun = str(row["raw_municipio"]).strip()
        cvegeo = str(row["cvegeo"]).strip().zfill(5)

        key = (raw_id_estado, normalize_text(raw_mun))

        if cvegeo in ref_idx.index:
            ref_row = ref_idx.loc[cvegeo]
            override_map[key] = dict(
                geo_cvegeo=cvegeo,
                geo_cve_estado=ref_row["cve_estado"],
                geo_cve_mun=ref_row["cve_mun"],
                geo_state=ref_row["state"],
                geo_municipality=ref_row["municipality"],
            )
        else:
            logger.warning("Override cvegeo %s not found in AGEEML", cvegeo)

    logger.info("Loaded %d override(s)", len(override_map))
    return override_map


def apply_overrides(
    df: pl.DataFrame, override_map: dict, dataset_name: str
) -> pl.DataFrame:
    """Fill geo_* columns for rows still unmatched, using geo_overrides.csv."""
    logger = logging.getLogger(__name__)
    if not override_map:
        return df

    pdf = df.to_pandas()
    import pandas as pd

    n = 0
    for idx in pdf[pdf["match_type"] == "unmatched"].index:
        est = pdf.at[idx, "cve_estado_raw"]
        est = str(est).zfill(2) if pd.notna(est) else ""
        mun = pdf.at[idx, "municipio_norm"]
        key = (est, mun)
        geo = override_map.get(key)
        if geo:
            for col, val in geo.items():
                pdf.at[idx, col] = val
            pdf.at[idx, "match_type"] = "override"
            n += 1

    if n:
        logger.info("[%s] Overrides applied: %d row(s) corrected", dataset_name, n)
    return pl.from_pandas(pdf)


# ---------------------------------------------------------------------------
# 5. Assign sentinel geo-codes
# ---------------------------------------------------------------------------
def assign_sentinels(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Assign sentinel geo-codes for known-unresolvable geography.

    Sentinel rules:
        id_estado == 33          -> cvegeo = 99998 (unknown state)
        municipio = "sin municipio de referencia" -> cve_mun = 998
        municipio = "se desconoce"               -> cve_mun = 999
    """
    logger = logging.getLogger(__name__)
    pdf = df.to_pandas()
    import pandas as pd

    is_unknown_state = pdf["id_estado"] == 33

    # Unknown state (id_estado == 33)
    pdf.loc[is_unknown_state, "geo_cvegeo"] = "99998"
    pdf.loc[is_unknown_state, "geo_cve_estado"] = "99"
    pdf.loc[is_unknown_state, "geo_state"] = "unknown"
    pdf.loc[is_unknown_state, "match_type"] = "sentinel_unknown_state"
    pdf.loc[is_unknown_state, "geo_cve_mun"] = "998"
    pdf.loc[is_unknown_state, "geo_municipality"] = pd.NA

    for label, code in _SENTINEL_MUNI.items():
        mask = is_unknown_state & (pdf["municipio_norm"] == label)
        if mask.any():
            pdf.loc[mask, "geo_cve_mun"] = str(code).zfill(3)
            pdf.loc[mask, "geo_municipality"] = label

    # Municipality sentinel with KNOWN state (id_estado != 33)
    for label, code in _SENTINEL_MUNI.items():
        mask = (pdf["municipio_norm"] == label) & ~is_unknown_state
        if mask.any():
            cve_mun_str = str(code).zfill(3)
            pdf.loc[mask, "geo_cve_mun"] = cve_mun_str
            pdf.loc[mask, "geo_municipality"] = label
            pdf.loc[mask, "geo_cvegeo"] = pdf.loc[mask, "cve_estado_raw"] + cve_mun_str
            pdf.loc[mask, "geo_cve_estado"] = pdf.loc[mask, "cve_estado_raw"]
            pdf.loc[mask, "geo_state"] = pdf.loc[mask, "estado_norm"]
            pdf.loc[mask, "match_type"] = f"sentinel_muni_{code}"

    sentinel_mask = pdf["match_type"].str.startswith("sentinel", na=False)
    counts = pdf.loc[sentinel_mask, "match_type"].value_counts().to_dict()
    if counts:
        logger.info("[%s] Sentinels: %s", dataset_name, counts)

    return pl.from_pandas(pdf)


# ---------------------------------------------------------------------------
# 6. Assign unresolved rows to state 99
# ---------------------------------------------------------------------------
def assign_unresolved(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Assign remaining unmatched rows to state 99 (unresolved).

    Instead of dropping rows with no geo assignment (as in v2), we assign:
        cve_estado = 99, state = "unresolved", cve_mun = 999, cvegeo = 99999
    This preserves aggregate person-counts.
    """
    logger = logging.getLogger(__name__)
    pdf = df.to_pandas()

    unresolved_mask = pdf["match_type"] == "unmatched"
    n = int(unresolved_mask.sum())

    if n:
        pdf.loc[unresolved_mask, "geo_cvegeo"] = "99999"
        pdf.loc[unresolved_mask, "geo_cve_estado"] = "99"
        pdf.loc[unresolved_mask, "geo_state"] = "unresolved"
        pdf.loc[unresolved_mask, "geo_cve_mun"] = "999"
        pdf.loc[unresolved_mask, "geo_municipality"] = pdf.loc[unresolved_mask, "municipio_norm"]
        pdf.loc[unresolved_mask, "match_type"] = "unresolved_state_99"
        logger.info("[%s] Unresolved: %d rows assigned to state 99 (cvegeo=99999)", dataset_name, n)

    return pl.from_pandas(pdf)


# ---------------------------------------------------------------------------
# 7. Assemble final target schema
# ---------------------------------------------------------------------------
def assemble_output(df: pl.DataFrame) -> pl.DataFrame:
    """Map internal columns to TARGET_SCHEMA and sort."""
    out = df.select([
        pl.col("geo_cvegeo").cast(pl.Utf8).str.zfill(5).alias("cvegeo"),
        pl.col("geo_cve_estado").cast(pl.Utf8).str.strip_chars().str.zfill(2).alias("cve_estado"),
        pl.col("geo_state").alias("state"),
        pl.col("geo_cve_mun").cast(pl.Utf8).str.strip_chars().str.zfill(3).alias("cve_mun"),
        pl.col("geo_municipality").alias("municipality"),
        pl.col("hombres").alias("male"),
        pl.col("mujeres").alias("female"),
        pl.col("indeterminado").alias("undefined"),
        pl.col("total_personas").alias("total"),
        pl.col("anio").alias("year"),
        pl.col("mes").alias("month"),
        pl.col("id_estatus_victima").alias("status_id"),
    ])
    return out.sort(["cvegeo", "year", "month"])


# ---------------------------------------------------------------------------
# 8. Remove ghost duplicates
# ---------------------------------------------------------------------------
def aggregate_duplicates(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Aggregate rows that share (cvegeo, year, month, status_id).

    Duplicates arise when B1 state corrections reassign a raw record to
    a state that already has its own entry for that municipality-month
    (e.g., AGUASCALIENTES recorded under Jalisco is corrected to state 01,
    colliding with the real Aguascalientes row).  Person counts are summed;
    the first non-null state/municipality name is kept.
    """
    logger = logging.getLogger(__name__)
    key_cols = ["cvegeo", "year", "month", "status_id"]
    n_before = len(df)

    df = df.group_by(key_cols).agg([
        pl.col("cve_estado").first(),
        pl.col("state").first(),
        pl.col("cve_mun").first(),
        pl.col("municipality").first(),
        pl.col("male").sum(),
        pl.col("female").sum(),
        pl.col("undefined").sum(),
        pl.col("total").sum(),
    ]).select(TARGET_SCHEMA)

    n_removed = n_before - len(df)
    if n_removed:
        logger.info("[%s] Aggregated %d duplicate rows (summed counts)", dataset_name, n_removed)
    return df.sort(["cvegeo", "year", "month"])


# ---------------------------------------------------------------------------
# 9. Quality checks
# ---------------------------------------------------------------------------
def quality_check(out: pl.DataFrame, dataset_name: str) -> dict:
    """Run all checks; return a flat summary dict."""
    logger = logging.getLogger(__name__)
    qc = {"dataset": dataset_name}

    qc["schema_ok"] = out.columns == TARGET_SCHEMA
    qc["total_rows"] = len(out)

    # missingness
    qc["missing_cvegeo"] = out["cvegeo"].null_count()
    qc["missing_state"] = out["state"].null_count()

    # duplicates on (cvegeo, year, month, status_id)
    n_with_geo = out.filter(pl.col("cvegeo").is_not_null())
    qc["duplicates"] = len(n_with_geo) - len(
        n_with_geo.unique(subset=["cvegeo", "year", "month", "status_id"])
    )

    # temporal validity
    qc["invalid_year"] = int(out.filter(
        ~pl.col("year").is_between(2015, 2025)
    ).height)
    qc["invalid_month"] = int(out.filter(
        ~pl.col("month").is_between(1, 12)
    ).height)

    # sex/total consistency
    sex_sum = out["male"] + out["female"] + out["undefined"]
    qc["sex_total_mismatch"] = int((sex_sum != out["total"]).sum())

    qc["match_rate_pct"] = round(
        (qc["total_rows"] - qc["missing_cvegeo"]) / qc["total_rows"] * 100, 2
    )

    logger.info(
        "[%s] QC – schema=%s | rows=%d | match=%.1f%% | dups=%d | sex_err=%d",
        dataset_name, qc["schema_ok"], qc["total_rows"],
        qc["match_rate_pct"], qc["duplicates"], qc["sex_total_mismatch"],
    )
    return qc


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("RNPDNO CLEANING PIPELINE")
    logger.info("=" * 70)

    for d in (PROCESSED_DIR, LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # Build AGEEML reference
    ref = build_reference()

    # Load manual files
    corrections = load_state_corrections()
    override_map = load_overrides(ref)

    all_qc = []
    all_disc = []

    for ds_name, cfg in DATASETS.items():
        logger.info("\n" + "-" * 60)
        logger.info("Processing: %s", ds_name)
        logger.info("-" * 60)

        # 1. Load + normalise
        df = load_and_normalize(RAW_DIR / cfg["raw_file"], ds_name)

        # 2. Discrepancies
        disc = detect_discrepancies(df, ref, ds_name)
        all_disc.extend(disc)

        # 3. State corrections (B1, pre-join)
        df = apply_state_corrections(df, corrections, ds_name)

        # 4. Two-pass geo-join
        df = geo_join(df, ref, ds_name)

        # 5. Manual overrides
        df = apply_overrides(df, override_map, ds_name)

        # 6. Sentinel assignment
        df = assign_sentinels(df, ds_name)

        # 7. Assign unresolved rows to state 99
        df = assign_unresolved(df, ds_name)

        # 8. Assemble target schema
        out = assemble_output(df)

        # 9. Remove duplicates
        out = aggregate_duplicates(out, ds_name)

        # 10. Quality check
        all_qc.append(quality_check(out, ds_name))

        # 11. Write output
        out_path = PROCESSED_DIR / cfg["output_file"]
        out.write_csv(out_path)
        logger.info("[%s] -> %s  (%d rows)", ds_name, out_path.name, len(out))

    # Write quality report
    qc_df = pl.DataFrame(all_qc)
    qc_df.write_csv(LOG_DIR / "quality_report.csv")
    logger.info("Quality report: %s", LOG_DIR / "quality_report.csv")

    # Write discrepancies
    if all_disc:
        disc_df = pl.DataFrame(all_disc)
        disc_df.write_csv(MANUAL_DIR / "discrepancies.csv")
        logger.info("Discrepancies: %d groups -> %s", len(all_disc), MANUAL_DIR / "discrepancies.csv")

    # Cross-status consistency check
    logger.info("\n" + "=" * 70)
    logger.info("CROSS-STATUS CONSISTENCY CHECK")
    logger.info("=" * 70)

    total_df = pl.read_csv(PROCESSED_DIR / "rnpdno_total.csv")
    sub_dfs = [
        pl.read_csv(PROCESSED_DIR / "rnpdno_disappeared_not_located.csv"),
        pl.read_csv(PROCESSED_DIR / "rnpdno_located_alive.csv"),
        pl.read_csv(PROCESSED_DIR / "rnpdno_located_dead.csv"),
    ]
    total_persons = total_df["total"].sum()
    sub_persons = sum(d["total"].sum() for d in sub_dfs)
    diff = total_persons - sub_persons
    logger.info("Total file persons: %d", total_persons)
    logger.info("Sub-status sum:     %d", sub_persons)
    logger.info("Difference:         %d (%.3f%%)", diff, abs(diff) / sub_persons * 100 if sub_persons else 0)

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
