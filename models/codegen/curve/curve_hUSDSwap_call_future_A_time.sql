{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_call_future_a_time',
        pre_hook={
            'sql': 'create or replace function curve_husdswap_future_a_time_calldecodeudf as "io.iftech.sparkudf.hive.Curve_hUSDSwap_future_A_time_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        curve_husdswap_future_a_time_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "future_A_time", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]}', 'future_A_time') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604") and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10 and selector = "0x14052288" and selector_hash = abs(hash("0x14052288")) % 10

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
