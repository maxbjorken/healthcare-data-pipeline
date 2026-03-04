SELECT 
    encounter_id,
    patient_id,
    status,
    encounter_class,
    encounter_type_display,
    patient_reference,
    period_start,
    period_end,
    last_updated,
    ingested_at
FROM {{ ref('stg_hapi_fhir_encounters') }}
WHERE patient_id IS NOT NULL AND encounter_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY last_updated DESC) = 1