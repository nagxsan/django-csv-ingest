from rest_framework import serializers

class CSVUploadSerializer(serializers.Serializer):
    table_name = serializers.CharField(max_length=128)
    file = serializers.FileField()

    # Optional: let clients pass a strict flag (fail-fast on first error)
    strict = serializers.BooleanField(required=False, default=True)

