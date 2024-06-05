select
    i.codigo_do_investidor  as investidor_id,
    i.estado_civil as marital_status,
    i.genero  as gender,
    i.profissao as occupation,
    i.idade as age,
    i.uf_do_investidor as investidor_state,
    i.cidade_do_investidor as investidor_city,
    i.pais_do_investidor as investidor_country
from {{ source('staging', 'investidors') }}  as i