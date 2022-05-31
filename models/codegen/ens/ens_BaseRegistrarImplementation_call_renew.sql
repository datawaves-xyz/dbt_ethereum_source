{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_call_renew',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_renew_calldecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_renew_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        ens_baseregistrarimplementation_renew_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "renew", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "id", "type": "uint256"}, {"name": "duration", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]}', 'renew') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0xc475abff" and selector_hash = abs(hash("0xc475abff")) % 10

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
        data.input.id as id, data.input.duration as duration, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
