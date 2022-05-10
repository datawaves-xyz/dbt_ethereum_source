{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_call_isapprovedforall',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_isapprovedforall_calldecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_isApprovedForAll_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_baseregistrarimplementation_isapprovedforall_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [{"internalType": "address", "name": "owner", "type": "address"}, {"internalType": "address", "name": "operator", "type": "address"}], "name": "isApprovedForAll", "outputs": [{"internalType": "bool", "name": "", "type": "bool"}], "payable": false, "stateMutability": "view", "type": "function"}', 'isApprovedForAll') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0xe985e9c5" and selector_hash = abs(hash("0xe985e9c5")) % 10

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
