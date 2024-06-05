  {{ config(schema='silver') }}
 
select 
    hash(concat(UPPER(TRIM(t.stock)), UPPER(TRIM(t.investidor_id)), UPPER(TRIM(t.dt_transaction)))) as hk_transaction,
    current_timestamp() as load_dts,
    t.quantity,
    t.transaction_value,
    case when  t.transaction_type = 'C' then 'Buy'
            when t.transaction_type = 'V' then  'sell'
    else 'other' end as type_operation,
    case when t.transaction_channel = 'S' then 'Site'
            when t.transaction_channel = 'H' then 'homebroker'
    else 'other' end as channel
from {{ ref('stg_transactions')}} as t