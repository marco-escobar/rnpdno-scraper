"""Audit script for processed RNPDNO CSV files.

Verifies every row in the 4 processed CSVs against the AGEEML catalog.
Run after clean_pipeline.py to catch any geo-code errors.

Usage:
    python scripts/processing/audit_output.py
"""

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
AGEEML_PATH = PROJECT_ROOT / "data" / "external" / "ageeml_catalog.csv"

FILES = [
    "rnpdno_total.csv",
    "rnpdno_disappeared_not_located.csv",
    "rnpdno_located_alive.csv",
    "rnpdno_located_dead.csv",
]

SENTINEL_CVEGEO = {"99998", "99999"}
SENTINEL_CVE_MUN = {998, 999}


def load_ageeml():
    """Load AGEEML and return dict: cvegeo -> (cve_ent, cve_mun, nom_mun, nom_ent)."""
    catalog = {}
    with open(AGEEML_PATH, encoding="latin1") as f:
        for row in csv.DictReader(f):
            cvegeo = row["CVEGEO"].strip().strip('"')
            catalog[cvegeo] = {
                "cve_ent": row["CVE_ENT"].strip().strip('"'),
                "cve_mun": row["CVE_MUN"].strip().strip('"'),
                "nom_mun": row["NOM_MUN"].strip().strip('"'),
                "nom_ent": row["NOM_ENT"].strip().strip('"'),
            }
    return catalog


def audit_file(path, ageeml):
    """Audit one processed CSV. Returns list of error dicts."""
    errors = []
    row_count = 0
    total_persons = 0

    with open(path) as f:
        reader = csv.DictReader(f)

        # Schema check
        expected = ["cvegeo", "cve_estado", "state", "cve_mun", "municipality",
                     "male", "female", "undefined", "total", "year", "month", "status_id"]
        if reader.fieldnames != expected:
            errors.append({"check": "schema", "detail": f"Expected {expected}, got {reader.fieldnames}"})

        seen_keys = {}
        for i, row in enumerate(reader, start=2):  # line 2 = first data row
            row_count += 1
            cvegeo = str(int(float(row["cvegeo"]))).zfill(5) if row["cvegeo"] else ""
            cve_estado = row["cve_estado"]
            cve_mun_raw = row["cve_mun"]
            total = int(row["total"])
            male = int(row["male"])
            female = int(row["female"])
            undefined = int(row["undefined"])
            year = int(row["year"])
            month = int(row["month"])
            total_persons += total

            # 1. Sex-total consistency
            if male + female + undefined != total:
                errors.append({"check": "sex_total", "line": i, "cvegeo": cvegeo,
                               "detail": f"{male}+{female}+{undefined}={male+female+undefined} != {total}"})

            # 2. Temporal validity
            if year < 2015 or year > 2025:
                errors.append({"check": "year_range", "line": i, "cvegeo": cvegeo, "detail": f"year={year}"})
            if month < 1 or month > 12:
                errors.append({"check": "month_range", "line": i, "cvegeo": cvegeo, "detail": f"month={month}"})

            # 3. Duplicate check
            dup_key = (cvegeo, row["year"], row["month"], row["status_id"])
            if dup_key in seen_keys:
                errors.append({"check": "duplicate", "line": i, "cvegeo": cvegeo,
                               "detail": f"Duplicate of line {seen_keys[dup_key]}"})
            seen_keys[dup_key] = i

            # 4. AGEEML cross-check (skip sentinels)
            if cvegeo in SENTINEL_CVEGEO:
                continue
            try:
                cve_mun_int = int(float(cve_mun_raw))
            except (ValueError, TypeError):
                cve_mun_int = None

            if cve_mun_int in SENTINEL_CVE_MUN:
                continue

            if cvegeo not in ageeml:
                errors.append({"check": "ageeml_missing", "line": i, "cvegeo": cvegeo,
                               "detail": f"cvegeo {cvegeo} not in AGEEML"})
            else:
                entry = ageeml[cvegeo]
                # Verify cve_estado matches
                expected_estado = entry["cve_ent"]
                actual_estado = str(int(float(cve_estado))).zfill(2) if cve_estado else ""
                if actual_estado != expected_estado:
                    errors.append({"check": "estado_mismatch", "line": i, "cvegeo": cvegeo,
                                   "detail": f"cve_estado={actual_estado} but AGEEML says {expected_estado}"})
                # Verify cve_mun matches
                expected_mun = entry["cve_mun"]
                actual_mun = str(cve_mun_int).zfill(3) if cve_mun_int else ""
                if actual_mun != expected_mun:
                    errors.append({"check": "mun_mismatch", "line": i, "cvegeo": cvegeo,
                                   "detail": f"cve_mun={actual_mun} but AGEEML says {expected_mun}"})

    return errors, row_count, total_persons


def main():
    ageeml = load_ageeml()
    print(f"AGEEML loaded: {len(ageeml)} municipalities")
    print()

    all_ok = True
    totals = {}

    for fname in FILES:
        path = PROCESSED_DIR / fname
        if not path.exists():
            print(f"MISSING: {path}")
            all_ok = False
            continue

        errors, row_count, total_persons = audit_file(path, ageeml)
        totals[fname] = total_persons

        if errors:
            all_ok = False
            print(f"FAIL: {fname} ({row_count} rows, {total_persons:,} persons)")
            for e in errors[:20]:
                print(f"  [{e['check']}] line {e.get('line','?')} cvegeo={e.get('cvegeo','?')}: {e['detail']}")
            if len(errors) > 20:
                print(f"  ... and {len(errors) - 20} more errors")
        else:
            print(f"OK:   {fname} ({row_count} rows, {total_persons:,} persons)")

    # Cross-status consistency
    print()
    if all(f in totals for f in FILES):
        total_file = totals[FILES[0]]
        sub_sum = sum(totals[f] for f in FILES[1:])
        diff = total_file - sub_sum
        pct = abs(diff) / total_file * 100 if total_file else 0
        print(f"Cross-status: total={total_file:,} sub-sum={sub_sum:,} diff={diff} ({pct:.3f}%)")
        if pct > 1.0:
            print("  WARNING: >1% discrepancy")
            all_ok = False

    print()
    if all_ok:
        print("AUDIT PASSED")
        return 0
    else:
        print("AUDIT FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
