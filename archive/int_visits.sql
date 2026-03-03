SELECT
    -- PK för besöket
    md5(cast(v.visit_id as string)) as visit_pk,
    
    -- FK:er (Här skapas kopplingarna)
    md5(cast(v.doctor_id as string)) as doctor_fk,
    md5(cast(v.clinic_id as string)) as clinic_fk,
    md5(cast(v.visit_date_key as string)) as date_fk,
    md5(cast(v.diag_code as string)) as diagnosis_fk,
    md5(cast(v.patient_id as string)) as patient_fk, -- Exempel på ytterligare FK

    -- Berikad data (Här ser vi att de faktiskt sitter ihop)
    amount,
FROM {{ ref('stg_visits') }} v

