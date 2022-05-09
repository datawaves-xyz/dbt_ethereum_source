{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_evt_stoprampa',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_stoprampa_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_StopRampA_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.10.jar";'
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
        curve_sethswap_stoprampa_eventdecodeudf(unhex_data, topics_arr, '{"name": "StopRampA", "inputs": [{"type": "uint256", "name": "A", "indexed": false}, {"type": "uint256", "name": "t", "indexed": false}], "anonymous": false, "type": "event"}', 'StopRampA') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938" and selector_hash = abs(hash("0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938")) % 10

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
        data.input.*
    from base
)

select /*+ REPARTITION(50) */ *
from final
