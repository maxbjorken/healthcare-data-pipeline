

import polars as pl
import duckdb
import dlt
from prefect import task, flow
from datetime import datetime
from utils import run_dbt  


DB_PATH = "healthcare.db"

# --- TASKS ---

dict_csv_tables = {
    "stage_clinics": {"path": "data/raw_clinics.csv", "pk": "clinic_id"},
    "stage_patients": {"path": "data/raw_patients.csv", "pk": "patient_id"},
    "stage_diagnosis": {"path": "data/raw_diagnosis.csv", "pk": "diag_code"},
    "stage_date": {"path": "data/raw_date.csv", "pk": "date_key"},
    "stage_visits": {"path": "data/raw_visits.csv", "pk": "visit_id"},
    "stage_doctors": {"path": "data/raw_doctors.csv", "pk": "doctor_id"}
}
#Task for reading from csv-files and staging the data in DuckDB.


    #Task for reading from csv-files and staging the data in DuckDB.
@task(name="Stage tables from csv-files_dlt")
def stage_tables():
 
        pipeline = dlt.pipeline(
            pipeline_name="healthcare_ingestion",
            destination="duckdb", 
            dataset_name="main"  
        )

        for table_name, config in dict_csv_tables.items():
            df = pl.read_csv(config["path"], null_values=["NULL", "null", ""])
            
            df = df.with_columns([
                pl.lit(datetime.now()).alias("_ingested_at"),
                pl.lit(config["path"]).alias("_source_file")
            ])

            load_info = pipeline.run(
                df, 
                table_name=table_name, 
                write_disposition="merge", 
                primary_key=config["pk"]  # <--- Här hämtas rätt ID dynamiskt!
            )
            print(f"Loaded {table_name}: {load_info}")

        return list(dict_csv_tables.keys())
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