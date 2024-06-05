  {{ config(schema='silver') }}
 
 select 
    hash(UPPER(TRIM(st.stock))) as hk_stock,
    current_timestamp() as load_dts,
    st.stock_id as stock_id,
    st.due_date as due_date,
    st.stock_value as total_value,
    st.stock_type as type
from {{  ref('stg_stock')}} as st