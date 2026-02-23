import polars as pl
import duckdb
from prefect import task, flow
from datetime import datetime
from utils import run_dbt  # <--- Här hämtar du din färdiga funktion!


DB_PATH = "healthcare.db"

# --- TASKS ---

@task(name="Stage clinics", retries=2)
def stage_clinics():
    df = pl.read_csv("data/raw_clinics.csv")
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_clinics.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_CLINICS AS SELECT * FROM df")
    return "STAGE_CLINICS"

@task(name="Stage patients")
def stage_patients():
    df = pl.read_csv("data/raw_patients.csv")
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_patients.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_PATIENTS AS SELECT * FROM df")
    return "STAGE_PATIENTS"

@task(name="Stage diagnosis")
def stage_diagnosis():
    df = pl.read_csv("data/raw_diagnosis.csv")
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_diagnosis.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_DIAGNOSIS AS SELECT * FROM df")
    return "STAGE_DIAGNOSIS"

@task(name="Stage date")
def stage_date():
    df = pl.read_csv("data/raw_date.csv")
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_date.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_DATE AS SELECT * FROM df")
    return "STAGE_DATE"

@task(name="Stage Visits")    
def stage_visits():
    df = pl.read_csv("data/raw_visits.csv")
    df = pl.read_csv("data/raw_visits.csv", null_values=["NULL", "null", ""])
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_visits.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_VISITS AS SELECT * FROM df")
    return "STAGE_VISITS"

@task(name="Stage doctors")
def stage_doctors():
    df = pl.read_csv("data/raw_doctors.csv")
    df = df.with_columns([
    pl.lit(datetime.now()).alias("_ingested_at"),
    pl.lit("raw_doctors.csv").alias("_source_file")
])
    with duckdb.connect(DB_PATH) as con:
        con.execute("CREATE OR REPLACE TABLE STAGE_DOCTORS AS SELECT * FROM df")
    return "STAGE_DOCTORS"

# --- FLOW ---

@flow(name="Healthcare End-to-End Pipeline")
def run_healthcare_pipeline():
    # 1. Kör alla inläsningar (E & L)
    # Vi samlar resultaten för att visa Prefect att nästa steg beror på dessa
    c = stage_clinics()
    p = stage_patients()
    d = stage_diagnosis()
    dt = stage_date()
    v = stage_visits()
    doc = stage_doctors()  

    # 2. Kör dbt Transformation (T)
    # Vi skickar med wait_for för att garantera att dbt inte startar 
    # förrän ALLA inläsningar är helt klara (Success)
    run_dbt(wait_for=[c, p, d, dt, v, doc])

if __name__ == "__main__":
    # Istället för att bara köra flowet en gång, "serverar" vi det nu
    run_healthcare_pipeline.serve(
        name="healthcare-daily-deployment",
        cron="0 6 * * *",  # Körs varje morgon kl 06:00
        tags=["production", "healthcare"],
        description="Laddar 160M rader och bygger guld-stjärnan i dbt."
    )