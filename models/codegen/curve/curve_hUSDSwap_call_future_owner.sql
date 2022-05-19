{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_call_future_owner',
        pre_hook={
            'sql': 'create or replace function curve_husdswap_future_owner_calldecodeudf as "io.iftech.sparkudf.hive.Curve_hUSDSwap_future_owner_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
        }
    )
}}

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_husdswap_future_owner_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "future_owner", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "address"}]}', 'future_owner') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604") and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10 and selector = "0x1ec0cdc1" and selector_hash = abs(hash("0x1ec0cdc1")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        call_success,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        dt,
        data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
