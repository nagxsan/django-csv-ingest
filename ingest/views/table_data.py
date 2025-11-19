from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from ingest.utils.build_where_clause import build_where_clause
from ingest.utils.constants import ALLOWED_TABLES

from django.db import connection, DatabaseError
from django.db.utils import ProgrammingError
from django.core.paginator import Paginator, EmptyPage

class GetTableDataView(APIView):
    def get(self, request, *args, **kwargs):
        mode = kwargs.get("mode")
        if mode == "table":
            return self.get_table_data(request)
        elif mode == "relations":
            return self.get_relations(request)
        else:
            return Response({"error": "invalid mode"}, status=status.HTTP_400_BAD_REQUEST)

    def get_relations(self, request):
        with connection.cursor() as cur:
            cur.execute(
                """
                  SELECT table_name
                  FROM information_schema.tables
                  WHERE table_schema = 'public'  
                """
            )
            rows = cur.fetchall()

        existing_relations = {row[0] for row in rows}
        allowed_relations = [
            rel for rel in ALLOWED_TABLES
            if rel in existing_relations
        ]

        return Response(
            {
                "relations": allowed_relations,
                "count": len(allowed_relations)
            }
        )

    def get_table_data(self, request):
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

