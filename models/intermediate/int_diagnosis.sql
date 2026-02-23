SELECT
md5(cast(diag_code as string)) as diagnosis_pk,
diag_code,
description,
_ingested_at,
_source_file
FROM {{ ref('stg_diagnosis') }}