# warsaw-transit-pipeline

A real-time data engineering pipeline built for learning: it polls live Warsaw
public transit vehicle positions (api.um.warszawa.pl) and IMGW weather data,
streams them through Kafka and PySpark Structured Streaming into a Delta Lake
on SeaweedFS (S3-compatible storage), orchestrates the whole thing with
Airflow, and exposes the results for analysis via DuckDB and a Streamlit
dashboard — all running locally through Docker Compose.

Status: Phase 0
# warsaw-transit-pipeline
