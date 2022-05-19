{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_evt_cancelmultipleorders',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_cancelmultipleorders_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_CancelMultipleOrders_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        looksrare_looksrareexchange_cancelmultipleorders_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address", "internalType": "address"}, {"indexed": false, "name": "orderNonces", "type": "uint256[]", "internalType": "uint256[]"}], "name": "CancelMultipleOrders", "type": "event"}', 'CancelMultipleOrders') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0xfa0ae5d80fe3763c880a3839fab0294171a6f730d1f82c4cd5392c6f67b41732" and selector_hash = abs(hash("0xfa0ae5d80fe3763c880a3839fab0294171a6f730d1f82c4cd5392c6f67b41732")) % 10

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
        data.input.user as user, data.input.ordernonces as orderNonces
    from base
)

select /*+ REPARTITION(50) */ *
from final
