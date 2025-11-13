import io
import time
from django.urls import reverse
from rest_framework.test import APITestCase
from django.db import connection


class CSVLoadTestLarge(APITestCase):

    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS public.load_test_table;
                CREATE TABLE public.load_test_table(
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    qty INTEGER NOT NULL
                );
            """)

    def generate_large_csv_stream(self, rows=100_000):
        """
        Stream-generate CSV into a BytesIO buffer.
        Avoids building large Python lists in memory.
        """
        buffer = io.StringIO()
        buffer.write("name,qty\n")
        for i in range(rows):
            buffer.write(f"Item_{i},{i % 100}\n")
        buffer.seek(0)
        return io.BytesIO(buffer.getvalue().encode("utf-8"))

    def test_large_csv_ingestion_streaming(self):
        # --- Generate CSV ---
        rows = 100_000
        csv_stream = self.generate_large_csv_stream(rows)

        # --- Hit the API ---
        url = reverse("upload-csv")

        start = time.time()

        resp = self.client.post(
            url,
            data={
                "table_name": "load_test_table",
                "file": csv_stream,
                "strict": True,
            },
            format="multipart",
        )

        duration = time.time() - start

        # --- Assertions ---
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], rows)

        # Optional performance expectation:
        # COPY should handle 100k rows in a few seconds.
        self.assertTrue(duration < 10, f"Ingestion took too long: {duration} seconds")

        print(f"\nLoaded {rows} rows in {duration:.2f}s")

