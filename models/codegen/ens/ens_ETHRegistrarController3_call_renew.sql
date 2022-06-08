{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller3_call_renew',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller3_renew_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController3_renew_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_ethregistrarcontroller3_renew_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "renew", "constant": false, "payable": true, "stateMutability": "payable", "inputs": [{"name": "name", "type": "string"}, {"name": "duration", "type": "uint256"}], "outputs": []}', 'renew') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5") and address_hash = abs(hash(lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5"))) % 10 and selector = "0xacf1a841" and selector_hash = abs(hash("0xacf1a841")) % 10

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
        data.input.name as name, data.input.duration as duration
    from base
)

select /*+ REPARTITION(50) */ *
from final
