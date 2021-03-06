{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_evt_orderapprovedpartone',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_orderapprovedpartone_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_OrderApprovedPartOne_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev2_orderapprovedpartone_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": false, "name": "exchange", "type": "address"}, {"indexed": true, "name": "maker", "type": "address"}, {"indexed": false, "name": "taker", "type": "address"}, {"indexed": false, "name": "makerRelayerFee", "type": "uint256"}, {"indexed": false, "name": "takerRelayerFee", "type": "uint256"}, {"indexed": false, "name": "makerProtocolFee", "type": "uint256"}, {"indexed": false, "name": "takerProtocolFee", "type": "uint256"}, {"indexed": true, "name": "feeRecipient", "type": "address"}, {"indexed": false, "name": "feeMethod", "type": "uint8"}, {"indexed": false, "name": "side", "type": "uint8"}, {"indexed": false, "name": "saleKind", "type": "uint8"}, {"indexed": false, "name": "target", "type": "address"}], "name": "OrderApprovedPartOne", "type": "event"}', 'OrderApprovedPartOne') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0x90c7f9f5b58c15f0f635bfb99f55d3d78fdbef3559e7d8abf5c81052a5276622" and selector_hash = abs(hash("0x90c7f9f5b58c15f0f635bfb99f55d3d78fdbef3559e7d8abf5c81052a5276622")) % 10

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
        data.input.hash as hash, data.input.exchange as exchange, data.input.maker as maker, data.input.taker as taker, data.input.makerrelayerfee as makerRelayerFee, data.input.takerrelayerfee as takerRelayerFee, data.input.makerprotocolfee as makerProtocolFee, data.input.takerprotocolfee as takerProtocolFee, data.input.feerecipient as feeRecipient, data.input.feemethod as feeMethod, data.input.side as side, data.input.salekind as saleKind, data.input.target as target
    from base
)

select /*+ REPARTITION(50) */ *
from final
