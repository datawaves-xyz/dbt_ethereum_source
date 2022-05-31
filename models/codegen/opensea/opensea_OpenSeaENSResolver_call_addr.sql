{{
    config(
        materialized='table',
        file_format='parquet',
        alias='openseaensresolver_call_addr',
        pre_hook={
            'sql': 'create or replace function opensea_openseaensresolver_addr_calldecodeudf as "io.iftech.sparkudf.hive.Opensea_OpenSeaENSResolver_addr_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        opensea_openseaensresolver_addr_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "addr", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "node", "type": "bytes32"}], "outputs": [{"name": "", "type": "address"}]}', 'addr') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956") and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10 and selector = "0x3b3b57de" and selector_hash = abs(hash("0x3b3b57de")) % 10

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
        data.input.node as node, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
