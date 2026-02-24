import polars as pl
from datetime import datetime


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
        # Not ready for sql (yet)
        print(f"Hämtar data från SQL: {config.get('name')}")
        return [] # Dummy
    
    else:
        raise ValueError(f"Osupportad källtyp: {source_type}")