  {{ config(schema='silver') }}
 
select 
    hash(UPPER(TRIM(i.investidor_id))) as hk_investidor_id,
    i.investidor_state,
    i.investidor_city,
    i.investidor_country
from {{ ref('stg_investidors') }} as i