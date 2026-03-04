SELECT 
                encounter_id,
                REGEXP_REPLACE(raw_json->>'$.subject.reference', '^(Patient/|#)', '') AS patient_id,
                raw_json->>'$.status' AS status,
                raw_json->>'$.class' AS encounter_class,
                raw_json->>'$.type[0].coding[0].code' AS encounter_type_code,
                raw_json->>'$.type[0].coding[0].display' AS encounter_type_display,
                raw_json->>'$.subject.reference' AS patient_reference,
                CAST(raw_json->>'$.period.start' AS TIMESTAMP) AS period_start,
                CAST(raw_json->>'$.period.end' AS TIMESTAMP) AS period_end,
                CAST(raw_json->>'$.meta.lastUpdated' AS TIMESTAMP) AS last_updated,
                raw_json->>'$.meta.versionId' AS version_id,
                raw_json->>'$.serviceProvider.reference' AS service_provider_reference,
                ingested_at
            FROM {{ source('hapi_fhir_source', 'raw_encounters') }}
           