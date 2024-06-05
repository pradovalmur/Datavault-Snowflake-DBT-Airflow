{{ config(schema='silver') }}

select 
    hash(UPPER(TRIM(acc.account_id))) as hk_account,
    current_timestamp() as load_dts,
    acc.accession_date,
    acc.account_status,
    acc.transactions_last_12_months
from {{ ref('stg_account') }} as acc 