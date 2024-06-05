{{ config(schema='silver') }}

select 
    hash(concat(UPPER(TRIM(t.stock)), UPPER(TRIM(t.investidor_id)), UPPER(TRIM(t.dt_transaction)))) as hk_transaction,
    current_timestamp() as load_dts,
    hash(UPPER(TRIM(t.stock))) as hk_titulo,
    hash(UPPER(TRIM(t.investidor_id))) as hk_account
from {{ ref('stg_transactions') }} as t