Select
diag_code,
description,
    _ingested_at,
    _source_file
FROM {{ source('healthcare_source', 'stage_diagnosis') }}