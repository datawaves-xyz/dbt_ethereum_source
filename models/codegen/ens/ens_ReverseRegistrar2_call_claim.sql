{{
    config(
        materialized='table',
        file_format='parquet',
        alias='reverseregistrar2_call_claim',
        pre_hook={
            'sql': 'create or replace function ens_reverseregistrar2_claim_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ReverseRegistrar2_claim_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_reverseregistrar2_claim_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "claim", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "owner", "type": "address"}], "outputs": [{"name": "", "type": "bytes32"}]}', 'claim') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x084b1c3c81545d370f3634392de611caabff8148") and address_hash = abs(hash(lower("0x084b1c3c81545d370f3634392de611caabff8148"))) % 10 and selector = "0x1e83409a" and selector_hash = abs(hash("0x1e83409a")) % 10

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
        data.input.owner as owner, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
