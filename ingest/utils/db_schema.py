from django.db import connection

def get_table_schema(table_name: str):
    """
    Returns ordered schema for a public table:
    [
      {'column':'id','data_type':'integer','is_nullable':False},
      ...
    ]
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            [table_name],
        )
        rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Table '{table_name}' does not exist or has no columns.")

    return [
        {"column": r[0], "data_type": r[1], "is_nullable": (r[2] == "YES"), "default": r[3]}
        for r in rows
    ]


def normalize_pg_type(data_type: str) -> str:
    """
    Normalize to coarse types for validation.
    """
    t = data_type.lower()
    if any(x in t for x in ["int", "serial", "smallint", "bigint"]):
        return "int"
    if any(x in t for x in ["double", "numeric", "decimal", "real"]):
        return "float"
    if "boolean" in t:
        return "bool"
    if any(x in t for x in ["timestamp", "date", "time"]):
        return "datetime"
    if any(x in t for x in ["json", "jsonb"]):
        return "json"
    # text, varchar, uuid, inet, etc. -> string
    return "string"

