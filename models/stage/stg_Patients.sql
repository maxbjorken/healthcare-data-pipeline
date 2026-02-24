SELECT
patient_id,
[Name] as patient_name,
age,
city,
    _ingested_at,
    _source_file
    FROM {{ source('csv', 'stage_patients') }}
