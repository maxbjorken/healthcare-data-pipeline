{{ config(materialized='table') }}

WITH base_encounters AS (
    SELECT * FROM {{ ref('int_HF_ENCOUNTERS') }}
),
base_patients AS (
    SELECT * FROM {{ ref('int_HF_PATIENTS') }}
)

SELECT
    MD5(e.encounter_id) AS encounter_sk,
    MD5(e.patient_id) AS patient_sk,
    e.encounter_id,
    e.status,
    e.period_start,
    e.period_end,
    date_diff('minute', e.period_start, e.period_end) AS duration_minutes
FROM base_encounters e
LEFT JOIN base_patients p ON e.patient_id = p.patient_id