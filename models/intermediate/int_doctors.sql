SELECT
    -- Skapa PK:n som vi ska använda i hela projektet
    md5(cast(doctor_id as string)) as doctor_pk,
    doctor_id,
    doctor_name,
    specialty,
    _ingested_at
FROM {{ ref('stg_Doctors') }}