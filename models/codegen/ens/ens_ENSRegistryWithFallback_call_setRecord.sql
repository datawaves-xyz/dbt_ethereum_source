{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistrywithfallback_call_setrecord',
        pre_hook={
            'sql': 'create or replace function ens_ensregistrywithfallback_setrecord_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistryWithFallback_setRecord_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_ensregistrywithfallback_setrecord_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setRecord", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "node", "type": "bytes32"}, {"name": "owner", "type": "address"}, {"name": "resolver", "type": "address"}, {"name": "ttl", "type": "uint64"}], "outputs": []}', 'setRecord') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0xcf408823" and selector_hash = abs(hash("0xcf408823")) % 10

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
        data.input.node as node, data.input.owner as owner, data.input.resolver as resolver, data.input.ttl as ttl
    from base
)

select /*+ REPARTITION(50) */ *
from final
