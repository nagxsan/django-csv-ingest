import io
from django.db import connection, transaction
import logging

logger = logging.getLogger(__name__)

def sanitize_value(v):
    if v is None:
        return "\\N"
    s = str(v)

    # Remove all variations of newline
    s = s.replace("\r\n", " ")
    s = s.replace("\n", " ")
    s = s.replace("\r", " ")

    # Replace tabs
    s = s.replace("\t", " ")

    # Remove zero-width or invisible separators
    s = s.replace("\u200b", "")  # zero-width space
    s = s.replace("\u00a0", " ") # non-breaking space

    # Normalize unicode (important for dashes from Numbers/Excel)
    import unicodedata
    s = unicodedata.normalize("NFKC", s)

    return s

def bulk_copy_into(table_name: str, rows: list[dict], ordered_cols: list[str]):
    if not rows:
        return 0

    logger.info("Ordered cols: %s", ordered_cols)
    logger.info("Num of rows: %s", len(rows))

    # Re-render a CSV purely for COPY
    buf = io.StringIO()
    # No header in data for COPY FROM STDIN WITH CSV
    for r in rows:
        values = []
        for c in ordered_cols:
            v = r[c]
            if v is None:
                values.append(r"\N")  # Postgres NULL
            else:
                # # Escape CSV w/ quotes when needed; simplest is str and rely on DELIMITER
                # # Safer: write with csv module — but then need header control.
                # s = str(v)
                # s = s.replace("\\", "\\\\")
                # s = s.replace("\n", "\\n")
                # s = s.replace("\t", "\\t")
                s = sanitize_value(v)
                values.append(s)
        logger.info(values)
        buf.write("\t".join(values) + "\n")
    buf.seek(0)

    quoted_cols = [f'"{c}"' for c in ordered_cols]

    with transaction.atomic():
        with connection.cursor() as cur:
            # Use TEXT format (default) with DELIMITER = E'\t' to avoid field commas
            cur.copy_expert(
                f"COPY public.{table_name} ({', '.join(quoted_cols)}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')",
                buf,
            )
    return len(rows)

