
select
    N_NATIONKEY as nation_id,
	N_NAME as nation,
	N_REGIONKEY as region_id,
	N_COMMENT as comment
from {{ ref('stg_nations') }}