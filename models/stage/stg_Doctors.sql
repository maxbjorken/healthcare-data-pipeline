Select 
doctor_id,
doctor_name,
specialty,
    _ingested_at,
    _source_file
    FROM {{ source('healthcare_source', 'Stage_Doctors') }}