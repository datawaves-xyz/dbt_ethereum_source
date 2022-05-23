{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller2_call_registerwithconfig',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller2_registerwithconfig_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController2_registerWithConfig_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        ens_ethregistrarcontroller2_registerwithconfig_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "registerWithConfig", "constant": false, "payable": true, "stateMutability": "payable", "inputs": [{"name": "name", "type": "string"}, {"name": "owner", "type": "address"}, {"name": "duration", "type": "uint256"}, {"name": "secret", "type": "bytes32"}, {"name": "resolver", "type": "address"}, {"name": "addr", "type": "address"}], "outputs": []}', 'registerWithConfig') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xB22c1C159d12461EA124b0deb4b5b93020E6Ad16") and address_hash = abs(hash(lower("0xB22c1C159d12461EA124b0deb4b5b93020E6Ad16"))) % 10 and selector = "0xf7a16963" and selector_hash = abs(hash("0xf7a16963")) % 10

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
