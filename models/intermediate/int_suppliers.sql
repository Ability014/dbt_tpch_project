{{
    config(
        materialized='incremental',
        unique_key = 'supplier_id',
        incremetal_strategy = 'merge',
        merge_update_columns = ['supplier_name', 'supplier_address', 'supplier_phone', 'account_balance']
    )
}}

select
    S_SUPPKEY as supplier_id,
	S_NAME as supplier_name,
	S_ADDRESS as supplier_address,
	S_NATIONKEY as nation_id,
	S_PHONE as supplier_phone,
	S_ACCTBAL as account_balance,
	S_COMMENT as comment
from {{ ref('stg_suppliers') }}
{% if is_incremental() %}

  where (S_SUPPKEY)  > (select max(supplier_id) from {{ this }})

{% endif %}