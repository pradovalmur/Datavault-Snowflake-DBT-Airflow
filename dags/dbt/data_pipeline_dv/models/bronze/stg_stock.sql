SELECT distinct
    row_number() over(partition by concat(s.tipo_Titulo, '-', s.vencimento_do_titulo) order by s.VENCIMENTO_DO_TITULO ) as stock_id,
    concat(s.tipo_titulo,'-', s.vencimento_do_titulo) as stock,
    s.tipo_titulo as stock_type,
    s.vencimento_do_titulo as due_date,
    s.valor_do_titulo as stock_value
FROM {{ source('staging', 'transactions') }}  as s