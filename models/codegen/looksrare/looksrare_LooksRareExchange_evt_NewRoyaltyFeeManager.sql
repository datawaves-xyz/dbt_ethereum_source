{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_evt_newroyaltyfeemanager',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_newroyaltyfeemanager_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_NewRoyaltyFeeManager_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        looksrare_looksrareexchange_newroyaltyfeemanager_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "royaltyFeeManager", "type": "address", "internalType": "address"}], "name": "NewRoyaltyFeeManager", "type": "event"}', 'NewRoyaltyFeeManager') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0x80e3874461ebbd918ac3e81da0a92e5e51387d70f337237c9123e48d20e5a508" and selector_hash = abs(hash("0x80e3874461ebbd918ac3e81da0a92e5e51387d70f337237c9123e48d20e5a508")) % 10

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
        data.input.royaltyfeemanager as royaltyFeeManager
    from base
)

select /*+ REPARTITION(50) */ *
from final
