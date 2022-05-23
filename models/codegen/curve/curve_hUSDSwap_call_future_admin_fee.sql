{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_call_future_admin_fee',
        pre_hook={
            'sql': 'create or replace function curve_husdswap_future_admin_fee_calldecodeudf as "io.iftech.sparkudf.hive.Curve_hUSDSwap_future_admin_fee_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        curve_husdswap_future_admin_fee_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "future_admin_fee", "stateMutability": "view", "inputs": [], "outputs": [{"name": "", "type": "uint256"}]}', 'future_admin_fee') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604") and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10 and selector = "0xe3824462" and selector_hash = abs(hash("0xe3824462")) % 10

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
