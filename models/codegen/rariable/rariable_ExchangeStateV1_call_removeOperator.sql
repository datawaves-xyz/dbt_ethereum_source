{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangestatev1_call_removeoperator',
        pre_hook={
            'sql': 'create or replace function rariable_exchangestatev1_removeoperator_calldecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeStateV1_removeOperator_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        rariable_exchangestatev1_removeoperator_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "removeOperator", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "account", "type": "address"}], "outputs": []}', 'removeOperator') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8") and address_hash = abs(hash(lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8"))) % 10 and selector = "0xac8a584a" and selector_hash = abs(hash("0xac8a584a")) % 10

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
