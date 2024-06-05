 select
    i.codigo_do_investidor
    as account_id,
    i.data_de_adesao as accession_date,
    i.situacao_da_conta as account_status,
    i.operou_12_meses as transactions_last_12_months
from {{ source('staging', 'investidors') }} as i