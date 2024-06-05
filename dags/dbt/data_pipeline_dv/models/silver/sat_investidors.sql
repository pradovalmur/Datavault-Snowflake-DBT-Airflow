{{ config(schema='silver') }}
 
select 
    hash(UPPER(TRIM(i.investidor_id))) as hk_investidor_id,
    i.marital_status,
    i.Gender,
    i.occupation, 
    i.age
from {{ ref('stg_investidors') }} as i