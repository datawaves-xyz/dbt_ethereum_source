{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_evt_newtransferselectornft',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_newtransferselectornft_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_NewTransferSelectorNFT_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        looksrare_looksrareexchange_newtransferselectornft_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "transferSelectorNFT", "type": "address", "internalType": "address"}], "name": "NewTransferSelectorNFT", "type": "event"}', 'NewTransferSelectorNFT') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0x205d78ab41afe80bd6b6aaa5d7599d5300ff8690da3ab1302c1b552f7baf7d8c" and selector_hash = abs(hash("0x205d78ab41afe80bd6b6aaa5d7599d5300ff8690da3ab1302c1b552f7baf7d8c")) % 10

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
        data.input.transferselectornft as transferSelectorNFT
    from base
)

select /*+ REPARTITION(50) */ *
from final
