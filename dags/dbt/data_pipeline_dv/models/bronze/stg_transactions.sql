select 
    t.codigo_do_investidor as investidor_id,
    t.data_da_operacao as dt_transaction,
    t.tipo_titulo as stock_type,
    t.vencimento_do_titulo as stock_due,
    concat(t.TIPO_TITULO, t.VENCIMENTO_DO_TITULO) as stock,
    t.quantidade as quantity ,
    t.valor_da_operacao as transaction_value,
    case when t.tipo_da_operacao = 'V' then 'Sell'
         when t.tipo_da_operacao = 'C' then 'Buy' 
         else 'other' end as transaction_type,
    case when t.canal_da_operacao = 'S' then 'site'
         when t.canal_da_operacao = 'H' then 'homebroker'
         else 'other' end as transaction_channel
from {{ source('staging', 'transactions') }} as t