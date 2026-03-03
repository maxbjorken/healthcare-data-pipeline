SELECT
visit_id,
patient_id,
doctor_id,
clinic_id,
diag_code,
visit_date_key,
amount,
    _ingested_at,
    _source_file
FROM {{ source('csv', 'stage_visits') }}