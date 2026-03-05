import duckdb

def run_query():
    con = duckdb.connect("hapi_fhir.db")
    query = """
            SELECT 
                patient_id,
                raw_json->>'$.gender' AS gender,
                raw_json->>'$.name[0].family' AS last_name,
                raw_json->>'$.name[0].given[0]' AS first_name,
                raw_json->>'$.birthDate' AS birth_date
            FROM raw_patients
            WHERE (raw_json->>'$.name[0].family') IS NOT NULL
            LIMIT 20;
        """
    

    QUERY2 = """
            Select * FROM stg_hapi_fhir_conditions
            LIMIT 20;
                """

    print("Kör fråga mot hapi_fhir.db...\n")

    con.sql(QUERY2).show()

    con.close()

if __name__ == "__main__":
    run_query()