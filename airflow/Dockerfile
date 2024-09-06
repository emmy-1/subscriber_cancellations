FROM quay.io/astronomer/astro-runtime:12.0.0

# replace dbt-postgres with another supported adapter if you're using a different warehouse type
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-databricks && deactivate