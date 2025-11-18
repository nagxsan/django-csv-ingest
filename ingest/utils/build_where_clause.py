def build_where_clause(filters):
    where_parts = []
    params = []

    for key, value in filters.items():
        if "__icontains" in key:
            col = key.replace("__icontains", "")
            where_parts.append(f"{col} ILIKE %s")
            params.append(f"%{value}%")
        elif "__gte" in key:
            col = key.replace("__gte", "")
            where_parts.append(f"{col} >= %s")
            params.append(value)
        elif "__lte" in key:
            col = key.replace("__lte", "")
            where_parts.append(f"{col} <= %s")
            params.append(value)
        elif "__in" in key:
            col = key.replace("__in", "")
            vals = value.split(",")
            placeholders = ", ".join(["%s"] * len(vals))
            where_parts.append(f"{col} IN ({placeholders})")
            params.extend(vals)
        else:
            where_parts.append(f"{key} = %s")
            params.append(value)

    where_clause = " AND ".join(where_parts)
    if where_clause:
        where_clause = f"WHERE {where_clause}"

    return where_clause, params
