{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_call_admin_fee',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_admin_fee_calldecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_admin_fee_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_sethswap_admin_fee_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "admin_fee", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]}', 'admin_fee') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0xfee3f7f9" and selector_hash = abs(hash("0xfee3f7f9")) % 10

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
