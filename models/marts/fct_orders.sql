{{
    config(
        materialized='incremental',
        unique_key = 'orderline_key',
        incremetal_strategy = 'merge',
        merge_update_columns = ['return_flag', 'line_status']
    )
}}

with orderlineitems as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.order_id', 'line_number']) }} as orderline_key,
        o.order_id,
        line_number,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        order_status,
        order_date,
        TRY_CAST(CONCAT(LEFT(order_date, 4), SUBSTR(order_date, 6, 2), SUBSTR(order_date, 9, 2)) AS INTEGER) As order_date_key,
        order_priority,
        clerk,
        ship_priority,
        {{ dbt_utils.generate_surrogate_key(['part_id', 'supplier_id']) }} as partsupp_key,
        quantity,
        extended_price,
        li.total_price,
        discount,
        tax,
        return_flag,
        line_status,
        ship_date,
        commit_date,
        receipt_date,
        ship_instruction,
        ship_mode,
        li.comment
    from {{ ref('int_orders') }} o
    left join {{ ref('int_lineitems') }} li
    on o.order_id=li.order_id
)
select
    *
from orderlineitems
{% if is_incremental() %}

  where (order_id+line_number)  > (select max(order_id+line_number) from {{ this }})

{% endif %}