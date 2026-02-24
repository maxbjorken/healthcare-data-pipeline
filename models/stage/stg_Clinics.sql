Select 
    Clinic_ID,
    Clinic_Name,
    Region,
    _ingested_at,
    _source_file
FROM {{ source('csv', 'stage_clinics') }}