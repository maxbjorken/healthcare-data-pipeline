import duckdb

def run_query():
    con = duckdb.connect("hapi_fhir.db")
    query = """
            SELECT * FROM RAW_PATIENTS
             LIMIT 5
        """

    print("Kör fråga mot hapi_fhir.db...\n")

    con.sql(query).show()

    con.close()

if __name__ == "__main__":
    run_query()