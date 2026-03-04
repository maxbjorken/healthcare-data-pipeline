import duckdb
import pandas as pd

def verify_ingestion():
    # Anslut till din huvud-databas
    conn = duckdb.connect("healthcare.db")
    
    print("=== 1. Kontrollerar Tabell-listan ===")
    tables = conn.execute("SHOW TABLES").df()
    print(tables)
    
    # Kolla om vår nya tabell finns i listan
    target_table = "stage_diagnosis_descriptions"
    if target_table in tables['name'].values:
        print(f"\n✅ SUCCESS: '{target_table}' hittades!")
        
        print(f"\n=== 2. Innehåll i {target_table} (Topp 5) ===")
        # Här ser vi de svenska namnen och beskrivningarna
        df_sql = conn.execute(f"SELECT * FROM {target_table} LIMIT 5").df()
        print(df_sql)
        
        print("\n=== 3. Metadata-check ===")
        # Kontrollera att vår ingestion_engine satte rätt käll-info
        metadata = conn.execute(f"""
            SELECT _source_file, COUNT(*) as antal_rader 
            FROM {target_table} 
            GROUP BY _source_file
        """).df()
        print(metadata)
        
    else:
        print(f"\n❌ ERROR: Hittade inte '{target_table}'.")
        print("Kontrollera om den hamnade under ett annat namn eller schema.")

    conn.close()

if __name__ == "__main__":
    verify_ingestion()