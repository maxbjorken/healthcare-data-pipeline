import polars as pl
import duckdb
import dlt
from prefect import task, flow
from datetime import datetime
from dbt_utils import run_dbt  
from data_sources.csv_sources import CSV_SOURCES as csv_tables
from ingestion_engine import get_data as ing_engine
from data_sources.sql_sources import SQL_SOURCES as sql_tables
from Get_FH_Data import ingest_fhir_tables

DB_PATH = "hapi_fhir.db"

ALL_SOURCES = {**csv_tables, **sql_tables}

# --- TASKS ---
@task(name="Stage tables_Universal", description="Stages all tables from CSV and SQL sources into DuckDB")
def stage_tables():
 
        pipeline = dlt.pipeline(
            pipeline_name="healthcare_ingestion",
            destination=dlt.destinations.duckdb("hapi_fhir.db"), 
            dataset_name="main"  
        )

        for table_name, config in ALL_SOURCES.items():
            df = ing_engine(config)  
            load_info = pipeline.run(
                df, 
                table_name=table_name, 
                write_disposition="replace", 
                primary_key=config["pk"]  # <--- Här hämtas rätt ID dynamiskt!
            )
            print(f"Loaded {table_name}: {load_info}")

        return list(csv_tables.keys())

@task(name="Ingest FHIR API")
def task_fhir_ingestion():
    return ingest_fhir_tables(db_path=DB_PATH)

# --- FLOW ---

@flow(name="Healthcare End-to-End Pipeline")
def run_healthcare_pipeline():
    # 1. Run all data ingestion tasks in parallel (P) and wait for them to complete
    #  c = stage_tables.submit() not in use right now
    a = task_fhir_ingestion.submit()

    # 2. Run dbt (T) after all data ignestion tasks are completed
    run_dbt(wait_for=[a])

if __name__ == "__main__":
    run_healthcare_pipeline.serve(
        name="healthcare-daily-deployment",
        tags=["production", "healthcare"],
        description="Data ingestion from CSV-files"
    )