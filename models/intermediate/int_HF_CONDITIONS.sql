Select 
    patient_id,
    condition_id,
    condition_code,
    condition_display,
    clinical_status_code,
    clinical_status_display,
    verification_status_code,
    verification_status_display,
    encounter_id,
    category_code,
    patient_reference,
    last_updated,
    version_id,
    ingested_at
FROM {{ ref('stg_hapi_fhir_conditions') }}
WHERE patient_id IS NOT NULL AND condition_id IS NOT NULL
qualify row_number() over (partition by condition_id order by last_updated desc) = 1