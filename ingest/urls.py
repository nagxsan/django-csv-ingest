from django.urls import path
from .views import UploadCSVView, GetTableDataView

urlpatterns = [
    path("upload-csv/", UploadCSVView.as_view(), name="upload-csv"),
    path("get-table-data/", GetTableDataView.as_view(), name="get-table-data"),
]

