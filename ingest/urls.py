from django.urls import path
from .views import UploadCSVView, GetTableDataView

urlpatterns = [
    path("upload-csv/", UploadCSVView.as_view(), name="upload-csv"),
    path("get-table-data/", GetTableDataView.as_view(), {"mode": "table"}, name="get-table-data"),
    path("get-relations/", GetTableDataView.as_view(), {"mode": "relations"}, name="get-relations")
]

