# Django CSV Ingestion Service

A lightweight CSV ingestion microservice built with **Django**, **Django REST Framework**, and **PostgreSQL**.

This service exposes a single endpoint that:

- Accepts a CSV file
- Validates each row against the **actual PostgreSQL schema**
- Automatically handles type casting (int, float, bool, datetime, json, etc.)
- Skips auto-generated columns (e.g., `id`, `created_at`)
- Inserts valid rows using **PostgreSQL COPY** for high performance
- Supports strict and non-strict validation modes

Perfect for ETL pipelines, admin CSV uploads, and bulk ingestion systems.

---

# 🚀 Features

- Schema-driven validation (reads schema from Postgres)
- Handles 100k–1M rows efficiently (COPY-based ingestion)
- JSON, numeric, boolean, datetime support
- Auto-detects serial/identity/default columns
- CSV validation with good error diagnostics
- Strict vs non-strict ingestion modes
- Fully tested with Django’s test suite

---

# 📂 Project Structure

