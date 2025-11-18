from django.urls import reverse
from rest_framework.test import APITestCase
from django.db import connection


class TableDataAPITests(APITestCase):

    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            # Create test table
            cur.execute("""
                DROP TABLE IF EXISTS public.products_query_test;
                CREATE TABLE public.products_query_test (
                    id BIGSERIAL PRIMARY KEY,
                    sku TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    in_stock BOOLEAN NOT NULL,
                    category TEXT
                );
            """)

            # Insert sample rows
            sample_rows = [
                ("A100", 10, True, "face"),
                ("A101", 20, False, "face"),
                ("B200", 30, True, "body"),
                ("C300", 40, True, "hair"),
                ("C301", 50, False, "hair"),
            ]
            for r in sample_rows:
                cur.execute("""
                    INSERT INTO public.products_query_test (sku, price, in_stock, category)
                    VALUES (%s, %s, %s, %s)
                """, r)

    # --------------------------------------------------------------------
    # 1. Basic fetch
    # --------------------------------------------------------------------
    def test_basic_fetch(self):
        url = reverse("get-table-data")
        resp = self.client.get(url, {"table": "products_query_test"})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["total_rows"], 5)
        self.assertEqual(len(resp.data["results"]), 5)

    # --------------------------------------------------------------------
    # 2. Pagination
    # --------------------------------------------------------------------
    def test_pagination(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "page": 2, "limit": 2})
        
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["page"], 2)
        self.assertEqual(resp.data["limit"], 2)
        self.assertEqual(len(resp.data["results"]), 2)

    # --------------------------------------------------------------------
    # 3. Page out-of-range
    # --------------------------------------------------------------------
    def test_page_out_of_range(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "page": 999})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Page", resp.data["detail"])

    # --------------------------------------------------------------------
    # 4. Sorting
    # --------------------------------------------------------------------
    def test_sorting_order_by(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "order_by": "price"})
        prices = [row["price"] for row in resp.data["results"]]

        self.assertEqual(prices, sorted(prices))

    # --------------------------------------------------------------------
    # 5. Filter: exact match
    # --------------------------------------------------------------------
    def test_filter_exact_match(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "category": "face"})
        results = resp.data["results"]

        self.assertEqual(len(results), 2)
        for r in results:
            self.assertEqual(r["category"], "face")

    # --------------------------------------------------------------------
    # 6. Filter: icontains search
    # --------------------------------------------------------------------
    def test_filter_icontains(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "sku__icontains": "C3"})
        results = resp.data["results"]

        self.assertEqual(len(results), 2)
        self.assertTrue(all("C3" in r["sku"].upper() for r in results))

    # --------------------------------------------------------------------
    # 7. Filter: price >= (gte)
    # --------------------------------------------------------------------
    def test_filter_gte(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "price__gte": "40"})
        results = resp.data["results"]

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["price"] >= 40 for r in results))

    # --------------------------------------------------------------------
    # 8. Filter: price <= (lte)
    # --------------------------------------------------------------------
    def test_filter_lte(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "price__lte": "20"})
        results = resp.data["results"]

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["price"] <= 20 for r in results))

    # --------------------------------------------------------------------
    # 9. Filter: id__in
    # --------------------------------------------------------------------
    def test_filter_in(self):
        url = reverse("get-table-data")

        # ids inserted = 1, 2, 3, 4, 5
        resp = self.client.get(url, {"table": "products_query_test", "id__in": "1,5"})
        results = resp.data["results"]

        self.assertEqual(len(results), 2)
        ids = {r["id"] for r in results}
        self.assertEqual(ids, {1, 5})

    # --------------------------------------------------------------------
    # 10. Invalid table name
    # --------------------------------------------------------------------
    def test_invalid_table(self):
        url = reverse("get-table-data")
        resp = self.client.get(url, {"table": "fake_table"})

        # your API will probably throw a psycopg2 error → 400
        self.assertEqual(resp.status_code, 400)

    # --------------------------------------------------------------------
    # 11. Unknown filter field should not break SQL
    # --------------------------------------------------------------------
    def test_unknown_filter(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {"table": "products_query_test", "does_not_exist": "x"})
        self.assertEqual(resp.status_code, 400)

    # --------------------------------------------------------------------
    # 12. SQL injection attempt should fail harmlessly
    # --------------------------------------------------------------------
    def test_sql_injection_attempt(self):
        url = reverse("get-table-data")

        resp = self.client.get(url, {
            "table": "products_query_test",
            "sku": "A100'; DROP TABLE products_query_test; --"
        })

        # Should return 0 rows, NOT execute the injection
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data["total_rows"], 0)

        # Verify table still exists
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products_query_test;")
            count = cur.fetchone()[0]

        self.assertEqual(count, 5)
