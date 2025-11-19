# ============================
# Base Image (Miniconda)
# ============================
FROM continuumio/miniconda3:latest

# Create working directory
WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create conda environment
RUN conda env remove -n django-csv || true
RUN conda env create -f environment.yml

# Activate env for all future RUN + CMD
SHELL ["conda", "run", "-n", "django-csv", "/bin/bash", "-c"]

# Ensure environment is activated on container start
ENV CONDA_DEFAULT_ENV=django-csv
ENV PATH=/opt/conda/envs/django-csv/bin:$PATH

# Install gunicorn inside the environment
RUN conda run -n django-csv pip install gunicorn

# Copy project files
COPY . .

# Expose Django port
EXPOSE 8000

# Run Django via Gunicorn
CMD ["conda", "run", "--no-capture-output", "-n", "django-csv", "gunicorn", "csv_ingest.wsgi:application", "--bind", "0.0.0.0:8000"]

