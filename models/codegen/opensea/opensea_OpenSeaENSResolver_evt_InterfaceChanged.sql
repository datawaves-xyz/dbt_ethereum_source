{{
    config(
        materialized='table',
        file_format='parquet',
        alias='openseaensresolver_evt_interfacechanged',
        pre_hook={
            'sql': 'create or replace function opensea_openseaensresolver_interfacechanged_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_OpenSeaENSResolver_InterfaceChanged_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_openseaensresolver_interfacechanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": true, "name": "interfaceID", "type": "bytes4"}, {"indexed": false, "name": "implementer", "type": "address"}], "name": "InterfaceChanged", "type": "event"}', 'InterfaceChanged') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956") and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10 and selector = "0x7c69f06bea0bdef565b709e93a147836b0063ba2dd89f02d0b7e8d931e6a6daa" and selector_hash = abs(hash("0x7c69f06bea0bdef565b709e93a147836b0063ba2dd89f02d0b7e8d931e6a6daa")) % 10

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
        data.input.node as node, data.input.interfaceid as interfaceID, data.input.implementer as implementer
    from base
)

select /*+ REPARTITION(50) */ *
from final
