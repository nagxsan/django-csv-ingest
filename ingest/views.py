from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import CSVUploadSerializer
from .utils.db_schema import get_table_schema
from .utils.csv_validator import validate_csv
from .utils.db_insert import bulk_copy_into

# Optional: harden surface by whitelisting tables
ALLOWED_TABLES = None  # e.g., {"users", "orders"} or None for all public tables

class UploadCSVView(APIView):
    def post(self, request):
        serializer = CSVUploadSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        table = serializer.validated_data["table_name"]
        strict = serializer.validated_data["strict"]
        file_obj = serializer.validated_data["file"]

        if ALLOWED_TABLES is not None and table not in ALLOWED_TABLES:
            return Response({"detail": "Table not allowed"}, status=status.HTTP_403_FORBIDDEN)

        try:
            schema = get_table_schema(table)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        try:
            rows, diag = validate_csv(file_obj, schema, strict=strict)
        except ValueError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        ordered_cols = [c["column"] for c in schema]
        # Drop columns not present in CSV (they’d be missing/None); we validated requireds above
        # Ensure insert order aligns with DB schema order
        insertable_cols = []
        for col in schema:
            colname = col["column"]
            default = col["default"]
            if default and ("nextval(" in default or "now()" in default):
                continue
            insertable_cols.append(colname)
        try:
            inserted = bulk_copy_into(table, rows, insertable_cols)
        except Exception as e:
            return Response({"detail": f"Insert failed: {e}"}, status=status.HTTP_400_BAD_REQUEST)

        return Response(
            {
                "table": table,
                "inserted_rows": inserted,
                "diagnostics": diag,
            },
            status=status.HTTP_201_CREATED,
        )

