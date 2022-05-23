{{
    config(
        materialized='table',
        file_format='parquet',
        alias='reverseregistrar2_call_setname',
        pre_hook={
            'sql': 'create or replace function ens_reverseregistrar2_setname_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ReverseRegistrar2_setName_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        ens_reverseregistrar2_setname_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setName", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "name", "type": "string"}], "outputs": [{"name": "", "type": "bytes32"}]}', 'setName') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x084b1c3c81545d370f3634392de611caabff8148") and address_hash = abs(hash(lower("0x084b1c3c81545d370f3634392de611caabff8148"))) % 10 and selector = "0xc47f0027" and selector_hash = abs(hash("0xc47f0027")) % 10

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

select /*+ REPARTITION(50) */ *
from final
