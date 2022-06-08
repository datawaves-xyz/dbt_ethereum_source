{{
    config(
        materialized='table',
        file_format='parquet',
        alias='dusdswap_evt_stoprampa',
        pre_hook={
            'sql': 'create or replace function curve_dusdswap_stoprampa_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_DUSDSwap_StopRampA_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_dusdswap_stoprampa_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "A", "type": "uint256"}, {"indexed": false, "name": "t", "type": "uint256"}], "name": "StopRampA", "type": "event"}', 'StopRampA') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c") and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10 and selector = "0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938" and selector_hash = abs(hash("0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938")) % 10

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
        data.input.a as A, data.input.t as t
    from base
)

select /*+ REPARTITION(50) */ *
from final
