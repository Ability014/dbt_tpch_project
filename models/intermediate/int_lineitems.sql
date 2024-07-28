{{
    config(
        materialized='incremental',
        unique_key = ['order_id', 'line_number'],
        incremetal_strategy = 'merge',
        merge_update_columns = ['return_flag', 'line_status']
    )
}}

select
    L_ORDERKEY as order_id,
	L_PARTKEY as part_id,
	L_SUPPKEY as supplier_id,
	L_LINENUMBER as line_number,
	L_QUANTITY as quantity,
	L_EXTENDEDPRICE as extended_price,
    L_EXTENDEDPRICE * (1-L_DISCOUNT) * (1+L_TAX) as total_price,
	L_DISCOUNT as discount,
	L_TAX as tax,
	L_RETURNFLAG as return_flag,
	L_LINESTATUS as line_status,
	L_SHIPDATE as ship_date,
	L_COMMITDATE as commit_date,
	L_RECEIPTDATE as receipt_date,
	L_SHIPINSTRUCT as ship_instruction,
	L_SHIPMODE as ship_mode,
	L_COMMENT as comment
from {{ ref('stg_lineitems') }}
{% if is_incremental() %}

  where (L_ORDERKEY + L_LINENUMBER)  > (select max(order_id+line_number) from {{ this }})

{% endif %}