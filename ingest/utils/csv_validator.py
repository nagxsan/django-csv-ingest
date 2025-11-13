import csv
import io
import json
from datetime import datetime

from .db_schema import normalize_pg_type

def _to_bool(val: str):
    t = val.strip().lower()
    if t in ("true", "t", "1", "yes", "y"): return True
    if t in ("false", "f", "0", "no", "n"): return False
    raise ValueError("invalid boolean")

def _to_int(val: str):
    return int(val.strip())

def _to_float(val: str):
    return float(val.strip())

def _to_datetime(val: str):
    # be pragmatic; add formats as needed
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            pass
    raise ValueError("invalid datetime")

def _to_json(val: str):
    return json.loads(val)

CASTERS = {
    "bool": _to_bool,
    "int": _to_int,
    "float": _to_float,
    "datetime": _to_datetime,
    "json": _to_json,
    "string": lambda v: v,  # no-op
}

def validate_csv(file_obj, schema, strict=True):
    """
    Returns (validated_rows:list[dict], diagnostics:dict)
    Validates header names, nullability, and attempts type casting.
    """
    content = file_obj.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))

    # Expected columns from DB (keep order)
    # db_cols = [c["column"] for c in schema]
    ignored_required_cols = []
    for col in schema:
        if (col["default"] is not None and "nextval(" in str(col["default"])):
            ignored_required_cols.append(col["column"])

    db_cols = [c["column"] for c in schema if c["column"] not in ignored_required_cols]

    # Header alignment
    csv_cols = reader.fieldnames or []
    missing = [c for c in db_cols if c not in csv_cols]
    extra = [c for c in csv_cols if c not in db_cols]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")
    # Extra columns allowed — we’ll ignore them during insert

    # Build per-column validators
    validators = {}
    nullables = {}
    for col in db_cols:
        info = next(x for x in schema if x["column"] == col)
        validators[col] = CASTERS.get(normalize_pg_type(info["data_type"]), CASTERS["string"])
        nullables[col] = info["is_nullable"]

    validated_rows = []
    errors = []
    rownum = 1  # for 1-based indexing including header as line 1

    insertable_cols = []
    for col in schema:
        default = col["default"]
        colname = col["column"]
        if default and ("nextval(" in default or "now()" in default):
            continue
        insertable_cols.append(colname)

    for row in reader:
        rownum += 1
        clean = {}
        try:
            for col in db_cols:
                # Skip columns that should not be inserted (id, created_at defaults, identity, etc.)
                if col not in insertable_cols:
                    continue

                raw = row.get(col, "")

                # Handle empty or missing values
                if raw is None or raw == "":
                    if not nullables[col]:
                        raise ValueError(f"column '{col}' is NOT NULL but value is empty")
                    clean[col] = None
                    continue

                # Apply type validator (int, float, json, datetime, etc.)
                try:
                    clean[col] = validators[col](raw)
                except Exception as e:
                    raise ValueError(f"column '{col}' failed validation: {e}")

            validated_rows.append(clean)
        except Exception as e:
            msg = f"row {rownum}: {e}"
            if strict:
                raise ValueError(msg)
            errors.append(msg)

    diagnostics = {
        "rows_in_csv": rownum - 1,
        "validated_rows": len(validated_rows),
        "skipped_rows": len(errors),
        "extra_columns_ignored": extra,
        "missing_columns": missing,
        "errors": errors,
    }
    return validated_rows, diagnostics

