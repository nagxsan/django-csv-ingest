# Django CSV Ingestion Service

A Django service that accepts a CSV file, validates it against a PostgreSQL table's schema, and inserts the rows efficiently using PostgreSQL COPY.

This README explains how to run the project **locally with Conda** and **via Docker Compose**.

---

# 1️⃣ Run Locally Using Conda (Recommended)

## 1. Create Conda environment

```sh
conda env create -f environment.yml
conda activate django-csv
```

## 2. Create .env in root
```sh
DJANGO_SECRET_KEY=dev-secret
DJANGO_DEBUG=True
DB_NAME=csv_demo
DB_USER=csv_user
DB_PASSWORD=csvpass
DB_HOST=127.0.0.1
DB_PORT=5432
```

## 3. Start PostgreSQL via Docker
```sh
docker run -d --name pg \
  -p 5432:5432 \
  -e POSTGRES_USER=csv_user \
  -e POSTGRES_PASSWORD=csvpass \
  -e POSTGRES_DB=csv_demo \
  postgres:16
```

## 4. Apply migrations
```sh
python manage.py migrate
```

## 5. Start server
```sh
python manage.py runserver
```

API is available at http://localhost:8000

# 2️⃣ Run Using Docker Compose (Django + Postgres)

## 1. Start the stack

```sh
docker-compose up --build
```

## 2. Stop the stack
```sh
docker-compose down
```

API is available at http://localhost:8000

Endpoint: `POST /api/upload-csv/`

| Field        | Description                     |
| ------------ | ------------------------------- |
| `table_name` | Name of target PostgreSQL table |
| `file`       | CSV file                        |
| `strict`     | `true` or `false`               |

Example request:
```sh
curl -X POST http://localhost:8000/api/upload-csv/ \
  -F "table_name=products" \
  -F "strict=true" \
  -F "file=@products.csv"
```

To run tests navigate to home directory and run: `python manage.py test`

