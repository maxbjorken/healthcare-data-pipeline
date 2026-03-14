import polars as pl
import duckdb
import dlt
from prefect import task, flow
from datetime import datetime
from dbt_utils import run_dbt  
from data_sources.csv_sources import CSV_SOURCES as csv_tables
from ingestion_engine import get_data as ing_engine
from data_sources.sql_sources import SQL_SOURCES as sql_tables
from FHIR_API import ingest_fhir_tables
from Universal_Ingest import stage_tables

DB_PATH = "hapi_fhir.db"

@task(name="Stage tables_Universal", description="Stages all tables from CSV and SQL sources into DuckDB")
def task_stage_tables():
    return stage_tables()


@task(name="Ingest FHIR API")
def task_fhir_ingestion():
    return ingest_fhir_tables(db_path=DB_PATH)

# --- FLOW ---

@flow(name="Healthcare End-to-End Pipeline")
def run_healthcare_pipeline():
    # 1. Run all data ingestion tasks in parallel (P) and wait for them to complete
    #  c = stage_tables.submit() not in use right now (Use for staging tables from SQL and CSV sources)
    a = task_fhir_ingestion.submit()

    # 2. Run dbt (T) after all data ignestion tasks are completed
    run_dbt(wait_for=[a])

if __name__ == "__main__":
    run_healthcare_pipeline.serve(
        name="healthcare-daily-deployment",
        tags=["production", "healthcare"],
        description="Data ingestion from FHIR API"
    )