SELECT
    md5(cast(patient_id as string)) as patient_pk, 
    patient_id,
    patient_name,
    age,
    city,
    _ingested_at,
    _source_file
FROM {{ ref('stg_Patients') }}