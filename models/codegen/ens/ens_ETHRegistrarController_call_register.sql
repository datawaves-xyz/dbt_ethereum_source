{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller_call_register',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller_register_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController_register_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        ens_ethregistrarcontroller_register_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "register", "constant": false, "payable": true, "stateMutability": "payable", "inputs": [{"name": "name", "type": "string"}, {"name": "owner", "type": "address"}, {"name": "duration", "type": "uint256"}, {"name": "secret", "type": "bytes32"}], "outputs": []}', 'register') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xF0AD5cAd05e10572EfcEB849f6Ff0c68f9700455") and address_hash = abs(hash(lower("0xF0AD5cAd05e10572EfcEB849f6Ff0c68f9700455"))) % 10 and selector = "0x85f6d155" and selector_hash = abs(hash("0x85f6d155")) % 10

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
        data.input.name as name, data.input.owner as owner, data.input.duration as duration, data.input.secret as secret
    from base
)

select /*+ REPARTITION(50) */ *
from final
