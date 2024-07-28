{{
    config(
        materialized='incremental',
        unique_key = 'customer_key',
        incremetal_strategy = 'merge',
        merge_update_columns = ['customer_address', 'customer_phone', 'account_balance', 'market_segment', 'customer_name']
    )
}}

with customer_demography as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        customer_name,
        customer_address,
        customer_phone,
        account_balance,
        market_segment,
        nation,
        region,
        c.comment
    from {{ ref('int_customers') }} as c
    left join {{ ref('int_nations') }} as n
    on c.nation_id=n.nation_id
    left join {{ ref('int_regions') }} as r
    on n.region_id=r.region_id
)
select
    *
from customer_demography
{% if is_incremental() %}

  where customer_id  > (select max(customer_id) from {{ this }})

{% endif %}