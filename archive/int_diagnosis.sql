SELECT
    md5(cast(csv.diag_code as string)) as diagnosis_pk,
    csv.diag_code,
    -- Vi hämtar det svenska namnet från SQL-tabellen
    sql.diagnos_namn as diagnosis_name,
    -- Vi behåller din ursprungliga description kolumn
    csv.description,
    csv._ingested_at,
    csv._source_file
FROM {{ ref('stg_diagnosis') }} as csv
LEFT JOIN {{ ref('stg_diagnosis_descriptions') }} as sql
    ON csv.diag_code = sql.diag_code