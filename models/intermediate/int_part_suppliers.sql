{{
    config(
        materialized='incremental',
        unique_key = ['part_id', 'supplier_id'],
        incremetal_strategy = 'merge',
        merge_update_columns = ['available_quantity', 'supply_cost']
    )
}}

with partsupp_with_surrogate as (
select
    PS_PARTKEY as part_id,
	PS_SUPPKEY as supplier_id,
    row_number() over (partition by PS_PARTKEY order by PS_SUPPKEY) surr_partsupp,
	PS_AVAILQTY as available_quantity,
	PS_SUPPLYCOST as supply_cost,
	PS_COMMENT as comment
from {{ ref('stg_part_suppliers') }}
)
select
    *
from partsupp_with_surrogate
{% if is_incremental() %}

  where (part_id+surr_partsupp)  > (select max(part_id+surr_partsupp) from {{ this }})

{% endif %}
