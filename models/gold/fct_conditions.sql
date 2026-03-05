{{config (materialized='table') }}

SELECT
    MD5(c.condition_id) AS condition_sk,
    MD5(c.patient_id) AS patient_sk,
    COALESCE(MD5(c.encounter_id), '-1') AS encounter_sk,
    c.condition_id,
    c.patient_id,
    c.clinical_status_code,
    c.clinical_status_display,
    c.verification_status_code,
    c.verification_status_display,
    c.category_code,
    c.condition_code,
    c.condition_display,
    c.patient_reference,
    c.last_updated,
    c.version_id,
    c.ingested_at
FROM {{ ref('int_HF_CONDITIONS') }} c
LEFT JOIN {{ ref('int_HF_PATIENTS') }} p ON c.patient_id = p.patient_id
LEFT JOIN {{ ref('int_HF_ENCOUNTERS') }} e ON c.encounter_id = e.encounter_id
WHERE c.condition_id IS NOT NULL