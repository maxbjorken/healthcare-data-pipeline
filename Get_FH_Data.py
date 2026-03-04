import requests
import duckdb
import json

resources = ["Encounter", "Patient", "Condition"]

def ingest_fhir_tables():

    db_name = "hapi_fhir.db"
    for res in resources:
        
        url = f"http://hapi.fhir.org/baseR4/{res}?_count=1000&_sort=-_lastUpdated"

        res = res.lower()+"s"

        print(f"Hämtar data från: {url}")
        
        try:
        
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            entries = data.get('entry', [])
            print(f"Hittade {len(entries)} {res} i svaret från servern.")
            
            if len(entries) == 0:
                print("Ingen data hittades. Avbryter.")
                return

            print(f"Ansluter till databasen: {db_name}")
            con = duckdb.connect(db_name)

            con.execute(f"DROP TABLE IF EXISTS raw_{res}")
            
            con.execute(f"""
                CREATE TABLE raw_{res} (
                    {res}_id VARCHAR,
                    raw_json JSON,
                    ingested_at TIMESTAMP
                )
            """)
            

            inserted_count = 0
            for entry in entries:
                resource = entry.get('resource')
                
            
                if not resource:
                    continue
                
            
                encounter_id = resource.get('id', 'SAKNAR_ID')
                
            
                resource_str = json.dumps(resource)
                
            
                con.execute(
                    f"INSERT INTO raw_{res} VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [encounter_id, resource_str]
                )
                inserted_count += 1
                
            print(f"✅ Framgång! Satte in {inserted_count} rader i tabellen 'raw_{res}'.")

            con.close()
            
        except Exception as e:
            print(f"❌ Något gick fel under inläsningen: {e}")        

if __name__ == "__main__":
    ingest_fhir_tables()