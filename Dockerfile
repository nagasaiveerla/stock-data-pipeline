FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy scripts
COPY scripts/ /opt/airflow/scripts/
COPY dags/ /opt/airflow/dags/

# Ensure proper permissions
USER root
RUN chown -R airflow:root /opt/airflow/scripts/
RUN chmod +x /opt/airflow/scripts/main.py

USER airflow