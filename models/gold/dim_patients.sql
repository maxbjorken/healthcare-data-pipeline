{{ config(materialized='table') }}

Select 
md5(patient_id) AS Patient_PK,
Patient_ID,
Gender,
Last_Name,
First_Name,
Birthdate,
last_updated,CASE 
    WHEN Birthdate IS NOT NULL THEN DATE_DIFF('year', Birthdate, CURRENT_DATE)
    ELSE NULL
END AS Age
FROM {{ ref('int_HF_PATIENTS') }}