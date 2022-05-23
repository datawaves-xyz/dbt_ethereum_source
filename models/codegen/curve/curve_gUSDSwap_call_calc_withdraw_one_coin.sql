{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_call_calc_withdraw_one_coin',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_calc_withdraw_one_coin_calldecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_calc_withdraw_one_coin_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        curve_gusdswap_calc_withdraw_one_coin_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "calc_withdraw_one_coin", "stateMutability": "view", "inputs": [{"name": "_token_amount", "type": "uint256"}, {"name": "i", "type": "int128"}], "outputs": [{"name": "", "type": "uint256"}]}', 'calc_withdraw_one_coin') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0xcc2b27d7" and selector_hash = abs(hash("0xcc2b27d7")) % 10

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
