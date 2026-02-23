import polars as pl
import duckdb
from prefect import task, flow
from datetime import datetime
from utils import run_dbt  


DB_PATH = "healthcare.db"

# --- TASKS ---

dict_csv_tables = {
    "STAGE_CLINICS": "data/raw_clinics.csv",
    "STAGE_PATIENTS": "data/raw_patients.csv",
    "STAGE_DIAGNOSIS": "data/raw_diagnosis.csv",
    "STAGE_DATE": "data/raw_date.csv",
    "STAGE_VISITS": "data/raw_visits.csv",
    "STAGE_DOCTORS": "data/raw_doctors.csv"
}

#Task for reading from csv-files and staging the data in DuckDB.
@task(name="Stage tables from csv-files")
def stage_tables():
    for table_name, csv_path in dict_csv_tables.items():
        df = pl.read_csv(csv_path)
        df = df.with_columns([
            pl.lit(datetime.now()).alias("_ingested_at"), #Adding metadata columns to track when the data was ingested
            pl.lit(csv_path).alias("_source_file") #Adding metadata columns to track the source file for each record
        ])
        df = pl.read_csv(csv_path, null_values=["NULL", "null", ""]) #Handling null values in the CSV files by specifying them during the read operation. This ensures that they are correctly interpreted as nulls in the resulting DataFrame.
        with duckdb.connect(DB_PATH) as con:
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
   
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