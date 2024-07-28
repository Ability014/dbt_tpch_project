{{
    config(
        materialized='incremental',
        unique_key = 'part_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['part_retail_price']
    )
}}

select
    P_PARTKEY as part_id,
	P_NAME as part_name,
	P_MFGR as manufacturer,
	P_BRAND as part_brand,
	P_TYPE as part_type,
	P_SIZE as part_size,
	P_CONTAINER as container,
	P_RETAILPRICE as part_retail_price,
	P_COMMENT as comment
from {{ ref('stg_parts') }}
{% if is_incremental() %}

  where (P_PARTKEY)  > (select max(part_id) from {{ this }})

{% endif %}