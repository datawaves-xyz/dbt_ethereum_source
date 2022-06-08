{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_call_commit_transfer_ownership',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_commit_transfer_ownership_calldecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_commit_transfer_ownership_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_gusdswap_commit_transfer_ownership_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "commit_transfer_ownership", "stateMutability": "nonpayable", "inputs": [{"name": "_owner", "type": "address"}], "outputs": []}', 'commit_transfer_ownership') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0x6b441a40" and selector_hash = abs(hash("0x6b441a40")) % 10

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
        data.input._owner as _owner
    from base
)

select /*+ REPARTITION(50) */ *
from final
