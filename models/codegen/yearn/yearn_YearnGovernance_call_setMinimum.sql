{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_call_setminimum',
        pre_hook={
            'sql': 'create or replace function yearn_yearngovernance_setminimum_calldecodeudf as "io.iftech.sparkudf.hive.Yearn_YearnGovernance_setMinimum_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        yearn_yearngovernance_setminimum_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setMinimum", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "_minimum", "type": "uint256"}], "outputs": []}', 'setMinimum') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E") and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10 and selector = "0x3209e9e6" and selector_hash = abs(hash("0x3209e9e6")) % 10

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
        data.input._minimum as _minimum
    from base
)

select /*+ REPARTITION(50) */ *
from final
