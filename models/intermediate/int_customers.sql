{{
    config(
        materialized='incremental',
        unique_key = 'customer_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['customer_address', 'customer_phone', 'account_balance', 'market_segment', 'customer_name']
    )
}}

select
    C_CUSTKEY as customer_id,
	C_NAME customer_name,
	C_ADDRESS as customer_address,
	C_NATIONKEY as nation_id,
	C_PHONE as customer_phone,
	C_ACCTBAL as account_balance,
	C_MKTSEGMENT as market_segment,
	C_COMMENT as comment
from {{ ref('stg_customers') }}
{% if is_incremental() %}

  where C_CUSTKEY  > (select max(customer_id) from {{ this }})

{% endif %}