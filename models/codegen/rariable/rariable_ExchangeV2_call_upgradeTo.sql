{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangev2_call_upgradeto',
        pre_hook={
            'sql': 'create or replace function rariable_exchangev2_upgradeto_calldecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeV2_upgradeTo_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        rariable_exchangev2_upgradeto_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "upgradeTo", "stateMutability": "nonpayable", "inputs": [{"name": "newImplementation", "type": "address"}], "outputs": []}', 'upgradeTo') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6") and address_hash = abs(hash(lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6"))) % 10 and selector = "0x3659cfe6" and selector_hash = abs(hash("0x3659cfe6")) % 10

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
