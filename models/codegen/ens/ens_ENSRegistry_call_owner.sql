{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistry_call_owner',
        pre_hook={
            'sql': 'create or replace function ens_ensregistry_owner_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistry_owner_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_ensregistry_owner_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "owner", "constant": true, "payable": false, "inputs": [{"name": "node", "type": "bytes32"}], "outputs": [{"name": "", "type": "address"}]}', 'owner') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0x02571be3" and selector_hash = abs(hash("0x02571be3")) % 10

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
