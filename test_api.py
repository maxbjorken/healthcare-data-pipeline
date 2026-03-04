import requests
import duckdb
import json

def ingest_fhir_patients():

    db_name = "hapi_fhir.db"

    url = "http://hapi.fhir.org/baseR4/Patient?_count=1000&_sort=-_lastUpdated"
    
    print(f"Hämtar data från: {url}")
    
    try:
    
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        entries = data.get('entry', [])
        print(f"Hittade {len(entries)} patienter i svaret från servern.")
        
        if len(entries) == 0:
            print("Ingen data hittades. Avbryter.")
            return

        print(f"Ansluter till databasen: {db_name}")
        con = duckdb.connect(db_name)

        con.execute("DROP TABLE IF EXISTS raw_patients")
        
        con.execute("""
            CREATE TABLE raw_patients (
                patient_id VARCHAR,
                raw_json JSON,
                ingested_at TIMESTAMP
            )
        """)
        

        inserted_count = 0
        for entry in entries:
            resource = entry.get('resource')
            
         
            if not resource:
                continue
            
           
            patient_id = resource.get('id', 'SAKNAR_ID')
            
         
            resource_str = json.dumps(resource)
            
         
            con.execute(
                "INSERT INTO raw_patients VALUES (?, ?, CURRENT_TIMESTAMP)",
                [patient_id, resource_str]
            )
            inserted_count += 1
            
        print(f"✅ Framgång! Satte in {inserted_count} rader i tabellen 'raw_patients'.")

        con.close()
        
    except Exception as e:
        print(f"❌ Något gick fel under inläsningen: {e}")



def ingest_fhir_encounters():

    db_name = "hapi_fhir.db"

    url = "http://hapi.fhir.org/baseR4/Encounter?_count=1000&_sort=-_lastUpdated"
    
    print(f"Hämtar data från: {url}")
    
    try:
    
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        entries = data.get('entry', [])
        print(f"Hittade {len(entries)} encounters i svaret från servern.")
        
        if len(entries) == 0:
            print("Ingen data hittades. Avbryter.")
            return

        print(f"Ansluter till databasen: {db_name}")
        con = duckdb.connect(db_name)

        con.execute("DROP TABLE IF EXISTS raw_encounters")
        
        con.execute("""
            CREATE TABLE raw_encounters (
                encounter_id VARCHAR,
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
                "INSERT INTO raw_encounters VALUES (?, ?, CURRENT_TIMESTAMP)",
                [encounter_id, resource_str]
            )
            inserted_count += 1
            
        print(f"✅ Framgång! Satte in {inserted_count} rader i tabellen 'raw_encounters'.")

        con.close()
        
    except Exception as e:
        print(f"❌ Något gick fel under inläsningen: {e}")        

if __name__ == "__main__":
    ingest_fhir_patients()
    ingest_fhir_encounters()