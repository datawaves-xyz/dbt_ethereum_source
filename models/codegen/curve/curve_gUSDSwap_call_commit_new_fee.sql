{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_call_commit_new_fee',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_commit_new_fee_calldecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_commit_new_fee_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        curve_gusdswap_commit_new_fee_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "commit_new_fee", "stateMutability": "nonpayable", "inputs": [{"name": "new_fee", "type": "uint256"}, {"name": "new_admin_fee", "type": "uint256"}], "outputs": []}', 'commit_new_fee') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0x5b5a1467" and selector_hash = abs(hash("0x5b5a1467")) % 10

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
