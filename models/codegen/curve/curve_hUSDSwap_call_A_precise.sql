{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_call_a_precise',
        pre_hook={
            'sql': 'create or replace function curve_husdswap_a_precise_calldecodeudf as "io.iftech.sparkudf.hive.Curve_hUSDSwap_A_precise_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        curve_husdswap_a_precise_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "A_precise", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]}', 'A_precise') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604") and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10 and selector = "0x76a2f0f0" and selector_hash = abs(hash("0x76a2f0f0")) % 10

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
