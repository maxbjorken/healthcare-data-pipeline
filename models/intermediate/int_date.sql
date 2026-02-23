Select
md5(cast(date_key as string)) as date_pk,  -- Skapa en unik primärnyckel för datum
date_key,
full_date,
is_weekend,
_ingested_at,
_source_file
FROM {{ ref('stg_date') }}