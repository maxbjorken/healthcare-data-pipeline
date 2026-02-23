SELECT
patient_id,
[Name] as patient_name,
age,
city,
    _ingested_at,
    _source_file
    FROM {{ source('healthcare_source', 'stage_patients') }}
