{{
    config(
        materialized='table',
        file_format='parquet',
        alias='reverseregistrar1_call_node',
        pre_hook={
            'sql': 'create or replace function ens_reverseregistrar1_node_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ReverseRegistrar1_node_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_reverseregistrar1_node_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "node", "constant": true, "payable": false, "inputs": [{"name": "addr", "type": "address"}], "outputs": [{"name": "ret", "type": "bytes32"}]}', 'node') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x9062C0A6Dbd6108336BcBe4593a3D1cE05512069") and address_hash = abs(hash(lower("0x9062C0A6Dbd6108336BcBe4593a3D1cE05512069"))) % 10 and selector = "0xbffbe61c" and selector_hash = abs(hash("0xbffbe61c")) % 10

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
        data.input.addr as addr, data.output.output_ret as output_ret
    from base
)

select /*+ REPARTITION(50) */ *
from final
