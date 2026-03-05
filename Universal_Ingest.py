import polars as pl
import duckdb
import dlt
from prefect import task, flow
from data_sources.csv_sources import CSV_SOURCES as csv_tables
from ingestion_engine import get_data as ing_engine
from data_sources.sql_sources import SQL_SOURCES as sql_tables

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
