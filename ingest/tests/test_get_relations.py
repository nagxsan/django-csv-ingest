from django.test import TestCase
from django.db import connection
from rest_framework.test import APIClient
from django.urls import reverse
from ingest.utils.constants import ALLOWED_TABLES


class TestGetRelations(TestCase):

    @classmethod
    def setUpTestData(cls):
        # Create mock tables for testing
        # We only create some of the tables in ALLOWED_TABLES
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS products (id serial PRIMARY KEY);")
            cursor.execute("CREATE TABLE IF NOT EXISTS product_purchases (id serial PRIMARY KEY);")
            # This table exists in DB but is NOT in ALLOWED_TABLES
            cursor.execute("CREATE TABLE IF NOT EXISTS unknown_table (id serial PRIMARY KEY);")

    def setUp(self):
        self.client = APIClient()
        self.url = reverse("get-relations") + "?mode=relations"

    def test_relations_only_returns_allowed_existing_tables(self):
        resp = self.client.get(self.url)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("relations", resp.data)

        returned = resp.data["relations"]

        # Intersection between ALLOWED_TABLES and existing tables
        with connection.cursor() as c:
            c.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='public'
            """)
            existing_tables = {row[0] for row in c.fetchall()}

        expected_relations = [
            t for t in ALLOWED_TABLES if t in existing_tables
        ]

        self.assertListEqual(
            sorted(returned),
            sorted(expected_relations),
            msg="The endpoint should return only allowed tables that actually exist."
        )

    @classmethod
    def tearDownClass(cls):
        # Clean up tables
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS products CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS product_purchases CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS unknown_table CASCADE;")
        super().tearDownClass()
