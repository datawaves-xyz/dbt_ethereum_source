{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_call_updateexecutionmanager',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_updateexecutionmanager_calldecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_updateExecutionManager_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        looksrare_looksrareexchange_updateexecutionmanager_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "updateExecutionManager", "stateMutability": "nonpayable", "inputs": [{"name": "_executionManager", "type": "address"}], "outputs": []}', 'updateExecutionManager') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0xd4ff41dc" and selector_hash = abs(hash("0xd4ff41dc")) % 10

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
        data.input._executionmanager as _executionManager
    from base
)

select /*+ REPARTITION(50) */ *
from final
