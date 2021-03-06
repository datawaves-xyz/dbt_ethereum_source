{{
    config(
        materialized='table',
        file_format='parquet',
        alias='cryptopunksmarket_evt_punktransfer',
        pre_hook={
            'sql': 'create or replace function cryptopunks_cryptopunksmarket_punktransfer_eventdecodeudf as "io.iftech.sparkudf.hive.Cryptopunks_CryptoPunksMarket_PunkTransfer_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        cryptopunks_cryptopunksmarket_punktransfer_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "from", "type": "address"}, {"indexed": true, "name": "to", "type": "address"}, {"indexed": false, "name": "punkIndex", "type": "uint256"}], "name": "PunkTransfer", "type": "event"}', 'PunkTransfer') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB") and address_hash = abs(hash(lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"))) % 10 and selector = "0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8" and selector_hash = abs(hash("0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8")) % 10

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
        data.input.from as from, data.input.to as to, data.input.punkindex as punkIndex
    from base
)

select /*+ REPARTITION(50) */ *
from final
