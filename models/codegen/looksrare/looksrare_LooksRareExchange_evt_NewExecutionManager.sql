{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_evt_newexecutionmanager',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_newexecutionmanager_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_NewExecutionManager_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        looksrare_looksrareexchange_newexecutionmanager_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "executionManager", "type": "address", "internalType": "address"}], "name": "NewExecutionManager", "type": "event"}', 'NewExecutionManager') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0x36e2a376eabc3bc60cb88f29c288f53e36874a95a7f407330ab4f166b0905698" and selector_hash = abs(hash("0x36e2a376eabc3bc60cb88f29c288f53e36874a95a7f407330ab4f166b0905698")) % 10

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
