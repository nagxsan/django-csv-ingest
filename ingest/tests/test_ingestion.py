from django.urls import reverse
from rest_framework.test import APITestCase
from django.db import connection
import io
import json


class CSVIngestionTestSuite(APITestCase):

    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            # Base test table
            cur.execute("""
                DROP TABLE IF EXISTS public.products_test;
                CREATE TABLE public.products_test(
                    id BIGSERIAL PRIMARY KEY,
                    sku TEXT NOT NULL,
                    price NUMERIC(10,2) NOT NULL,
                    in_stock BOOLEAN NOT NULL,
                    tags JSONB,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)

            # Table with NOT NULL violations
            cur.execute("""
                DROP TABLE IF EXISTS public.notnull_test;
                CREATE TABLE public.notnull_test(
                    name TEXT NOT NULL,
                    qty INTEGER NOT NULL
                );
            """)

            # Table for JSON tests
            cur.execute("""
                DROP TABLE IF EXISTS public.jsontest;
                CREATE TABLE public.jsontest(
                    id BIGSERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
            """)

    # ----------------------------------------------------------
    # 1) Successful upload
    # ----------------------------------------------------------
    def test_successful_upload(self):
        content = (
            'sku,price,in_stock,tags,created_at\n'
            'A1,10.50,true,,2024-01-01 10:00:00\n'
        ).encode()


        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "products_test", "file": io.BytesIO(content), "strict": True},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], 1)

    # ----------------------------------------------------------
    # 2) Missing required column (sku missing)
    # ----------------------------------------------------------
    def test_missing_required_column(self):
        content = (
            "price,in_stock,tags,created_at\n"
            "10.5,true,,2024-01-01\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "products_test", "file": io.BytesIO(content), "strict": True},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("missing required columns", resp.data["detail"].lower())

    # ----------------------------------------------------------
    # 3) Bad datatype (price expects numeric)
    # ----------------------------------------------------------
    def test_bad_datatype(self):
        content = (
            "sku,price,in_stock,tags,created_at\n"
            "A1,NOTANUMBER,true,,2024-01-01\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "products_test", "file": io.BytesIO(content)},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("failed validation", resp.data["detail"])

    # ----------------------------------------------------------
    # 4) NOT NULL violation
    # ----------------------------------------------------------
    def test_not_null_violation(self):
        content = (
            "name,qty\n"
            ",10\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "notnull_test", "file": io.BytesIO(content), "strict": True},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("NOT NULL", resp.data["detail"])

    # ----------------------------------------------------------
    # 5) Invalid JSON field
    # ----------------------------------------------------------
    def test_invalid_json_field(self):
        content = (
            "data\n"
            "INVALID_JSON\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "jsontest", "file": io.BytesIO(content), "strict": True},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("failed validation", resp.data["detail"])

    # ----------------------------------------------------------
    # 6) Auto-increment PK should be ignored (id NOT required)
    # ----------------------------------------------------------
    def test_autoincrement_id_not_required(self):
        content = (
            "sku,price,in_stock,tags,created_at\n"
            "A2,20.0,true,,2024-02-01\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "products_test", "file": io.BytesIO(content)},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], 1)

    # ----------------------------------------------------------
    # 7) Extra CSV columns should be ignored
    # ----------------------------------------------------------
    def test_extra_columns_ignored(self):
        content = (
            "sku,price,in_stock,tags,created_at,extracol\n"
            "A3,12.00,true,,2024-03-01,SHOULD_BE_IGNORED\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "products_test", "file": io.BytesIO(content)},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], 1)

    # ----------------------------------------------------------
    # 8) Non-strict mode: skip bad rows, insert good ones
    # ----------------------------------------------------------
    def test_non_strict_skips_bad_rows(self):
        content = (
            "name,qty\n"
            "Pen,10\n"
            "Pencil,NOTANUMBER\n"     # bad row
            "Marker,5\n"
        ).encode()

        resp = self.client.post(
            reverse("upload-csv"),
            data={"table_name": "notnull_test", "file": io.BytesIO(content), "strict": False},
            format="multipart",
        )

        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], 2)  # Only Pen + Marker
        self.assertEqual(resp.data["diagnostics"]["skipped_rows"], 1)

