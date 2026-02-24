Select 
Date_Key,
Full_Date,
is_weekend,
    _ingested_at,
    _source_file
FROM {{ source('csv', 'stage_date') }}