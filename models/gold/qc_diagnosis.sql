{{ config(materialized='view') }}

WITH metrics AS (
    -- Kolla Conditions
    SELECT 
        'Conditions' AS category,
        COUNT(*) AS total_rows,
        COUNT(CASE WHEN patient_sk IS NULL OR patient_sk = '-1' THEN 1 END) AS missing_patient_keys,
        COUNT(CASE WHEN encounter_sk IS NULL OR encounter_sk = '-1' THEN 1 END) AS orphan_encounters
    FROM {{ ref('fct_conditions') }}

    UNION ALL

    -- Kolla Encounters
    SELECT 
        'Encounters' AS category,
        COUNT(*) AS total_rows,
        COUNT(CASE WHEN patient_sk IS NULL OR patient_sk = '-1' THEN 1 END) AS missing_patient_keys,
        0 AS orphan_encounters -- Inte applicerbart här på samma sätt
    FROM {{ ref('fct_encounters') }}
)

SELECT 
    category,
    total_rows,
    missing_patient_keys,
    orphan_encounters,
    -- Räkna ut felprocent
    ROUND((missing_patient_keys::FLOAT / NULLIF(total_rows, 0)) * 100, 2) AS error_rate_percent
FROM metrics