import duckdb
import json

def print_patient_hierarchy():

    con = duckdb.connect("hapi_fhir.db")

    query = """
        SELECT raw_json 
        FROM raw_patients 
        WHERE (raw_json->'$.name') IS NOT NULL 
        LIMIT 1
    """
   
    result = con.execute(query).fetchone()
    
    if result:
        raw_json_string = result[0]

        patient_data = json.loads(raw_json_string)
        
        print("--- HIERARKI FÖR EN PATIENT (FHIR JSON) ---")
        print(json.dumps(patient_data, indent=2))
        print("-------------------------------------------")
    else:
        print("Hittade ingen patient med namn i databasen.")

    con.close()

if __name__ == "__main__":
    print_patient_hierarchy()