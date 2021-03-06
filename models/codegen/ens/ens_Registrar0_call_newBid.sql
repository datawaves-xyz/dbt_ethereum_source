{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_call_newbid',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_newbid_calldecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_newBid_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_registrar0_newbid_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "newBid", "constant": false, "payable": true, "inputs": [{"name": "sealedBid", "type": "bytes32"}], "outputs": []}', 'newBid') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0xce92dced" and selector_hash = abs(hash("0xce92dced")) % 10

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
        data.input.sealedbid as sealedBid
    from base
)

select /*+ REPARTITION(50) */ *
from final
