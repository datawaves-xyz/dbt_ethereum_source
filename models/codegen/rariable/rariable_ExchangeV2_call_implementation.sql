{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangev2_call_implementation',
        pre_hook={
            'sql': 'create or replace function rariable_exchangev2_implementation_calldecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeV2_implementation_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        rariable_exchangev2_implementation_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "implementation", "stateMutability": "nonpayable", "inputs": [], "outputs": [{"name": "implementation_", "type": "address"}]}', 'implementation') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6") and address_hash = abs(hash(lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6"))) % 10 and selector = "0x5c60da1b" and selector_hash = abs(hash("0x5c60da1b")) % 10

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
        data.output.output_implementation_ as output_implementation_
    from base
)

select /*+ REPARTITION(50) */ *
from final
