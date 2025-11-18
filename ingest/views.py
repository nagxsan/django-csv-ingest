from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import CSVUploadSerializer
from .utils.db_schema import get_table_schema
from .utils.csv_validator import validate_csv
from .utils.db_insert import bulk_copy_into
from .utils.build_where_clause import build_where_clause

from django.db import connection, DatabaseError
from django.db.utils import ProgrammingError
from django.core.paginator import Paginator, EmptyPage

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

class GetTableDataView(APIView):
    def get(self, request):
        table = request.GET.get("table")
        if not table:
            return Response({"detail": "table parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        if ALLOWED_TABLES is not None and table not in ALLOWED_TABLES:
            return Response({"detail": "Table not allowed"}, status=status.HTTP_403_FORBIDDEN)


        page = int(request.GET.get("page", 1))
        limit = int(request.GET.get("limit", 10))
        order_by = request.GET.get("order_by", "id")

        reserved = ["table", "page", "limit", "order_by"]
        filters = {k: v for k, v in request.GET.items() if k not in reserved}

        where_clause, params = build_where_clause(filters)

        query = f"SELECT * FROM {table} {where_clause} ORDER BY {order_by}"

        try:
            with connection.cursor() as cur:
                cur.execute(query, params)
                columns = [col[0] for col in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        except ProgrammingError as e:
            # Typically invalid column, bad filter, or malformed SQL
            return Response(
                {"detail": f"Invalid query or filter: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST
            )

        except DatabaseError as e:
            # Any other Django DB-level error
            return Response(
                {"detail": f"Database error occurred: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        except Exception as e:
            # Final safety net so the API never crashes
            return Response(
                {"detail": f"Unexpected error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        paginator = Paginator(rows, limit)
        try:
            page_obj = paginator.page(page)
        except EmptyPage:
            return Response({"detail": "Page out of range"}, status=status.HTTP_400_BAD_REQUEST)

        return Response(
            {
                "page": page,
                "limit": limit,
                "total_rows": paginator.count,
                "total_pages": paginator.num_pages,
                "results": page_obj.object_list
            }
        )
