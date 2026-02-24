Select
diag_code,
description,
    _ingested_at,
    _source_file
FROM {{ source('csv', 'stage_diagnosis') }}