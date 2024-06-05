{{ config(schema='silver') }}

select 
    hash(UPPER(TRIM(acc.account_id))) as hk_account,
    current_timestamp() as load_dts,
    'account' as source,
    acc.account_id as account_id
from {{ ref('stg_account') }} as acc