SQL_SOURCES = {
    "stage_diagnosis_descriptions": {
        "name": "stage_diagnosis_descriptions",
        "connection": "external_source.db", 
        "query": "SELECT * FROM sql_diagnosis_lookup",
        "pk": "diag_code",
        "source_type": "sql"
    }
}