{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_call_setbreaker',
        pre_hook={
            'sql': 'create or replace function yearn_yearngovernance_setbreaker_calldecodeudf as "io.iftech.sparkudf.hive.Yearn_YearnGovernance_setBreaker_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        yearn_yearngovernance_setbreaker_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setBreaker", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "_breaker", "type": "bool"}], "outputs": []}', 'setBreaker') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E") and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10 and selector = "0x5c0aeb0e" and selector_hash = abs(hash("0x5c0aeb0e")) % 10

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
        data.input._breaker as _breaker
    from base
)

select /*+ REPARTITION(50) */ *
from final
