import polars as pl
import duckdb
import dlt
from prefect import task, flow
from datetime import datetime
from dbt_utils import run_dbt  
from data_sources.csv_sources import CSV_SOURCES as csv_tables
from ingestion_engine import get_data as ing_engine


DB_PATH = "healthcare.db"

# --- TASKS ---
@task(name="Stage tables")
def stage_tables():
 
        pipeline = dlt.pipeline(
            pipeline_name="healthcare_ingestion",
            destination="duckdb", 
            dataset_name="main"  
        )

        for table_name, config in csv_tables.items():
            df = ing_engine(config)  
            load_info = pipeline.run(
                df, 
                table_name=table_name, 
                write_disposition="merge", 
                primary_key=config["pk"]  # <--- Här hämtas rätt ID dynamiskt!
            )
            print(f"Loaded {table_name}: {load_info}")

        return list(csv_tables.keys())

# --- FLOW ---

@flow(name="Healthcare End-to-End Pipeline")
def run_healthcare_pipeline():
    # 1. Run all data ingestion tasks in parallel (P) and wait for them to complete
    c = stage_tables.submit()

    # 2. Run dbt (T) after all data ignestion tasks are completed
    run_dbt(wait_for=[c])

if __name__ == "__main__":
    run_healthcare_pipeline.serve(
        name="healthcare-daily-deployment",
        tags=["production", "healthcare"],
        description="Data ingestion from CSV-files"
    )