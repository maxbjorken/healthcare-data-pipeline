Select 
patient_id as 'Patiend_ID',
gender as 'Gender',
last_name as 'Last_Name',
first_name as 'First_Name',
birth_date as 'Birthdate',
last_updated as 'Last_Updated'
FROM {{ ref('stg_hapi_fihr_patients') }}
WHERE is_active = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY last_updated DESC) = 1