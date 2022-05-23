{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameauctioncontroller_evt_nameregistered',
        pre_hook={
            'sql': 'create or replace function ens_shortnameauctioncontroller_nameregistered_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameAuctionController_NameRegistered_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        ens_shortnameauctioncontroller_nameregistered_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "name", "type": "string", "internalType": "string"}, {"indexed": false, "name": "owner", "type": "address", "internalType": "address"}], "name": "NameRegistered", "type": "event"}', 'NameRegistered') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17") and address_hash = abs(hash(lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17"))) % 10 and selector = "0x1c6eac0e720ec22bb0653aec9c19985633a4fb07971cf973096c2f8e3c37c17f" and selector_hash = abs(hash("0x1c6eac0e720ec22bb0653aec9c19985633a4fb07971cf973096c2f8e3c37c17f")) % 10

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
