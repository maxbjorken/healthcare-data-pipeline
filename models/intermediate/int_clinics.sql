SELECT
md5(cast(clinic_id as string)) as clinic_pk,  -- Skapa en unik primärnyckel för kliniker
Clinic_ID,
clinic_name,
region,
_ingested_at,
_source_file
FROM {{ ref('stg_Clinics') }}