

select
    R_REGIONKEY as region_id,
	R_NAME as region,
	R_COMMENT as comment
from {{ ref('stg_regions') }}