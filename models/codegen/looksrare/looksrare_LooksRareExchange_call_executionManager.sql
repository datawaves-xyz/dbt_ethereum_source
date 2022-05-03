{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_call_executionmanager',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_executionmanager_calldecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_executionManager_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.4.jar";'
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
        looksrare_looksrareexchange_executionmanager_calldecodeudf(unhex_input, unhex_output, '{"inputs": [], "name": "executionManager", "outputs": [{"internalType": "contract IExecutionManager", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"}', 'executionManager') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x483abb9f"
    and selector_hash = abs(hash("0x483abb9f")) % 10

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
        data.input.*,
        data.output.*
    from base
)

select /*+ REPARTITION(1) */ *
from final
