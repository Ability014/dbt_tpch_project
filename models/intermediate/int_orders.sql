{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['order_status']
    )
}}

select
    O_ORDERKEY as order_id,
	O_CUSTKEY as customer_id,
	O_ORDERSTATUS as order_status,
	O_TOTALPRICE as total_price,
	O_ORDERDATE as order_date,
	O_ORDERPRIORITY as order_priority,
	O_CLERK as clerk,
	O_SHIPPRIORITY as ship_priority,
	O_COMMENT as comment
from {{ ref('stg_orders') }}
{% if is_incremental() %}

  where (O_ORDERKEY)  > (select max(order_id) from {{ this }})

{% endif %}