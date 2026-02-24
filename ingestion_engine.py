import polars as pl
from datetime import datetime
import duckdb

def get_data(config):
    source_type = config.get("source_type")
    
    if source_type == "csv":
        df = pl.read_csv(config["path"], null_values=["NULL", "null", ""])
        
        df = df.with_columns([
            pl.lit(datetime.now()).alias("_ingested_at"),
            pl.lit(config["path"]).alias("_source_file")
        ])
        
        return df.to_dicts()
    
    elif source_type == "sql":
        # Här ansluter vi till din nya external_source.db
        with duckdb.connect(config["connection"]) as conn:
            df = conn.execute(config["query"]).pl()
        
        # Lägg till metadata även här så dbt vet när datan hämtades
        df = df.with_columns([
            pl.lit(datetime.now()).alias("_ingested_at"),
            pl.lit("sql_server").alias("_source_file")
        ])
        return df.to_dicts()
    
    else:
        raise ValueError(f"Osupportad källtyp: {source_type}")