{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_evt_removeliquidityone',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_removeliquidityone_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_RemoveLiquidityOne_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.6.jar";'
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
        curve_sethswap_removeliquidityone_eventdecodeudf(unhex_data, topics_arr, '{"name": "RemoveLiquidityOne", "inputs": [{"type": "address", "name": "provider", "indexed": true}, {"type": "uint256", "name": "token_amount", "indexed": false}, {"type": "uint256", "name": "coin_amount", "indexed": false}], "anonymous": false, "type": "event"}', 'RemoveLiquidityOne') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567")
    and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10
    and selector = "0x9e96dd3b997a2a257eec4df9bb6eaf626e206df5f543bd963682d143300be310"
    and selector_hash = abs(hash("0x9e96dd3b997a2a257eec4df9bb6eaf626e206df5f543bd963682d143300be310")) % 10

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
