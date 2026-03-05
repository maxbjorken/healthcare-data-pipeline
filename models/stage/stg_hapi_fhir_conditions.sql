SELECT 
    condition_id,
    REGEXP_REPLACE(raw_json->>'$.subject.reference', '^(Patient/|#)', '') AS patient_id,
    REGEXP_REPLACE(raw_json->>'$.encounter.reference', '^(Encounter/|#)', '') AS encounter_id,
    raw_json->>'$.clinicalStatus.coding[0].code' AS clinical_status_code,
    raw_json->>'$.clinicalStatus.coding[0].display' AS clinical_status_display,
    raw_json->>'$.verificationStatus.coding[0].code' AS verification_status_code,
    raw_json->>'$.verificationStatus.coding[0].display' AS verification_status_display,
    raw_json->>'$.category[0].coding[0].code' AS category_code,
    raw_json->>'$.category[0].coding[0].display' AS category_display,
    raw_json->>'$.code.coding[0].code' AS condition_code,
    raw_json->>'$.code.coding[0].display' AS condition_display,
    raw_json->>'$.subject.reference' AS patient_reference,
    CAST(raw_json->>'$.onsetDateTime' AS TIMESTAMP) AS onset_datetime,
    CAST(raw_json->>'$.abatementDateTime' AS TIMESTAMP) AS abatement_datetime,
    CAST(raw_json->>'$.meta.lastUpdated' AS TIMESTAMP) AS last_updated,
    raw_json->>'$.meta.versionId' AS version_id,
    ingested_at
FROM {{ source('hapi_fhir_source', 'raw_conditions') }}
