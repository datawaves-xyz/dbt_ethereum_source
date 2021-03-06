{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_evt_rampa',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_rampa_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_RampA_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
        }
    )
}}

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_gusdswap_rampa_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "old_A", "type": "uint256"}, {"indexed": false, "name": "new_A", "type": "uint256"}, {"indexed": false, "name": "initial_time", "type": "uint256"}, {"indexed": false, "name": "future_time", "type": "uint256"}], "name": "RampA", "type": "event"}', 'RampA') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0xa2b71ec6df949300b59aab36b55e189697b750119dd349fcfa8c0f779e83c254" and selector_hash = abs(hash("0xa2b71ec6df949300b59aab36b55e189697b750119dd349fcfa8c0f779e83c254")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        contract_address,
        dt,
        data.input.old_a as old_A, data.input.new_a as new_A, data.input.initial_time as initial_time, data.input.future_time as future_time
    from base
)

select /*+ REPARTITION(50) */ *
from final
