{{
    config(
        materialized='table',
        file_format='parquet',
        alias='reverseregistrar2_call_addr_reverse_node',
        pre_hook={
            'sql': 'create or replace function ens_reverseregistrar2_addr_reverse_node_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ReverseRegistrar2_ADDR_REVERSE_NODE_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_reverseregistrar2_addr_reverse_node_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "ADDR_REVERSE_NODE", "constant": true, "payable": false, "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "bytes32"}]}', 'ADDR_REVERSE_NODE') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x084b1c3c81545d370f3634392de611caabff8148") and address_hash = abs(hash(lower("0x084b1c3c81545d370f3634392de611caabff8148"))) % 10 and selector = "0x7cf8a2eb" and selector_hash = abs(hash("0x7cf8a2eb")) % 10

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
