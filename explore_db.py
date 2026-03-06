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
            SELECT 
    c.patient_id AS id_fran_condition,
    c.patient_sk AS hash_fran_condition,
    p.patient_id AS id_fran_patient_tabell,
    p.patient_pk AS hash_fran_patient_tabell,
    c.condition_display
FROM fct_conditions c
LEFT JOIN dim_patients p ON c.patient_sk = p.patient_pk
WHERE p.patient_pk IS NULL
LIMIT 20;
                """

    print("Kör fråga mot hapi_fhir.db...\n")

    con.sql(QUERY2).show()

    con.close()

if __name__ == "__main__":
    run_query()