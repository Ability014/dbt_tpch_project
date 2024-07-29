{{
    config(
        materialized='incremental',
        unique_key = 'partsupp_key',
        incremetal_strategy = 'merge',
        merge_update_columns = ['available_quantity', 'supply_cost', 'part_retail_price', 'supplier_name', 'supplier_address', 'supplier_phone', 'account_balance']
    )
}}

with part_suppliers as (
    select
        {{ dbt_utils.generate_surrogate_key(['ps.part_id', 'ps.supplier_id']) }} as partsupp_key,
        ps.part_id,
        row_number() over (partition by ps.part_id order by ps.supplier_id) as surr_partsupp,
        ps.supplier_id,
        available_quantity,
        supply_cost,
        part_name,
        manufacturer,
        part_brand,
        part_type,
        part_size,
        container,
        part_retail_price,
        supplier_name,
        supplier_address,
        nation,
        supplier_phone,
        account_balance,
        ps.comment
    from {{ ref('int_part_suppliers') }} as ps
    left join {{ ref('int_parts') }} as p
    on ps.part_id=p.part_id
    left join {{ ref('int_suppliers') }} as s
    on ps.supplier_id=s.supplier_id
    left join {{ ref('int_nations') }} as n
    on s.nation_id=n.nation_id
)
select 
    *
from part_suppliers
{% if is_incremental() %}

  where (part_id+surr_partsupp)  > (select max(part_id+surr_partsupp) from {{ this }})

{% endif %}