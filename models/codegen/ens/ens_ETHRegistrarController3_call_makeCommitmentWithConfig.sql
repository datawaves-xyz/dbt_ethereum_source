{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller3_call_makecommitmentwithconfig',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller3_makecommitmentwithconfig_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController3_makeCommitmentWithConfig_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_ethregistrarcontroller3_makecommitmentwithconfig_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "makeCommitmentWithConfig", "constant": true, "payable": false, "stateMutability": "pure", "inputs": [{"name": "name", "type": "string"}, {"name": "owner", "type": "address"}, {"name": "secret", "type": "bytes32"}, {"name": "resolver", "type": "address"}, {"name": "addr", "type": "address"}], "outputs": [{"name": "", "type": "bytes32"}]}', 'makeCommitmentWithConfig') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5") and address_hash = abs(hash(lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5"))) % 10 and selector = "0x3d86c52f" and selector_hash = abs(hash("0x3d86c52f")) % 10

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