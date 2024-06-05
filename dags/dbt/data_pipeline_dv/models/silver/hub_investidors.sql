  {{ config(schema='silver') }}

select 
    hash(UPPER(TRIM(i.investidor_id))) as hk_investidor_id,
    current_timestamp() as load_dts,
    i.investidor_id as account_id,
    'investidor' as source
from {{ ref('stg_investidors') }} as i 