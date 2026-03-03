{{ config(
    materialized='table',
    description='Ren fakttabell för besök med endast nycklar och mått'
) }}

-- depends_on: {{ ref('dim_doctors') }}
-- depends_on: {{ ref('dim_clinics') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_diagnosis') }}
-- depends_on: {{ ref('dim_patients') }}

SELECT
    visit_pk,        -- Unik nyckel för raden
    doctor_fk,       -- Koppling till dim_doctors,
    clinic_fk,       -- Koppling till dim_clinics
    date_fk,         -- Koppling till dim_date
    diagnosis_fk,    -- Koppling till dim_diagnosis
    patient_fk,      -- Koppling till dim_patients
    amount,
    CASE WHEN(amount > 1000) THEN 'High' ELSE 'Low' END as amount_category, -- Exempel på berikning

    
    -- Mått och dimensionella attribut som hör till själva händelsen

FROM {{ ref('int_visits') }}