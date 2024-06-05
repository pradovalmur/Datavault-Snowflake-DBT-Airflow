  {{ config(schema='silver') }}
 
select 
    hash(UPPER(TRIM(acc.account_id))) as hk_account_investidor,
    i.investidor_id,
    acc.account_id
from {{ ref('stg_account') }} as acc
inner join {{ ref('stg_investidors') }} as i