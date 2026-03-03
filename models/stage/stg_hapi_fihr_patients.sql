SELECT 
                patient_id,
                raw_json->>'$.gender' AS gender,
                raw_json->>'$.name[0].family' AS last_name,
                raw_json->>'$.name[0].given[0]' AS first_name,
                CAST(raw_json->>'$.birthDate' AS DATE) AS birth_date,
                CAST(raw_json->>'$.meta.lastUpdated' AS TIMESTAMP) AS last_updated,
                raw_json->>'$.meta.versionId' AS version_id,
                ingested_at
            FROM {{ source('hapi_fhir_source', 'raw_patients') }}
            WHERE (raw_json->>'$.name[0].family') IS NOT NULL /* Exclude patients without last name, HAPI FIHR can include some wierd data */