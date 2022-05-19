{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistrywithfallback_call_setowner',
        pre_hook={
            'sql': 'create or replace function ens_ensregistrywithfallback_setowner_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistryWithFallback_setOwner_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        ens_ensregistrywithfallback_setowner_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setOwner", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "node", "type": "bytes32"}, {"name": "owner", "type": "address"}], "outputs": []}', 'setOwner') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0x5b0fc9c3" and selector_hash = abs(hash("0x5b0fc9c3")) % 10

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
        data.input.node as node, data.input.owner as owner
    from base
)

select /*+ REPARTITION(50) */ *
from final
