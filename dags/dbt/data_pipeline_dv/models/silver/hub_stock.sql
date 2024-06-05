{{ config(schema='silver') }}

select 
    hash(UPPER(TRIM(st.stock))) as hk_stock,
    current_timestamp() as load_dts,
    'stock' as source,
    st.stock_id as stock_id
from {{ ref('stg_stock') }} as st