from django.urls import reverse
from rest_framework.test import APITestCase
from django.db import connection
import io

class UploadCSVTests(APITestCase):

    @classmethod
    def setUpTestData(cls):
        with connection.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS public._t;
                CREATE TABLE public._t(
                  name text NOT NULL,
                  qty  integer NOT NULL
                )
            """)

    def test_upload_ok(self):
        content = b"name,qty\nPen,10\nPencil,5\n"
        url = reverse("upload-csv")
        resp = self.client.post(
            url,
            data={"table_name": "_t", "file": io.BytesIO(content), "strict": True},
            format="multipart",
        )
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.data["inserted_rows"], 2)

