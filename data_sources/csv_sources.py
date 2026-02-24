# csv_config.py

CSV_SOURCES = {
    "stage_clinics": {
        "name": "stage_clinics",
        "path": "data/raw_clinics.csv",
        "pk": "clinic_id",
        "source_type": "csv" 
    },
    "stage_patients": {
        "name": "stage_patients",
        "path": "data/raw_patients.csv",
        "pk": "patient_id",
        "source_type": "csv"
    },
    "stage_diagnosis": {
        "name": "stage_diagnosis",
        "path": "data/raw_diagnosis.csv",
        "pk": "diag_code",
        "source_type": "csv"
    },
    "stage_date": {
        "name": "stage_date",
        "path": "data/raw_date.csv",
        "pk": "date_key",
        "source_type": "csv"
    },
    "stage_visits": {
        "name": "stage_visits",
        "path": "data/raw_visits.csv",
        "pk": "visit_id",
        "source_type": "csv"
    },
    "stage_doctors": {
        "name": "stage_doctors",
        "path": "data/raw_doctors.csv",
        "pk": "doctor_id",
        "source_type": "csv"
    }
}