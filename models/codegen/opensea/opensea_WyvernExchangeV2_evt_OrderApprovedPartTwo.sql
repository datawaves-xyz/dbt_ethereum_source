{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_evt_orderapprovedparttwo',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_orderapprovedparttwo_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_OrderApprovedPartTwo_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev2_orderapprovedparttwo_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": false, "name": "howToCall", "type": "uint8"}, {"indexed": false, "name": "calldata", "type": "bytes"}, {"indexed": false, "name": "replacementPattern", "type": "bytes"}, {"indexed": false, "name": "staticTarget", "type": "address"}, {"indexed": false, "name": "staticExtradata", "type": "bytes"}, {"indexed": false, "name": "paymentToken", "type": "address"}, {"indexed": false, "name": "basePrice", "type": "uint256"}, {"indexed": false, "name": "extra", "type": "uint256"}, {"indexed": false, "name": "listingTime", "type": "uint256"}, {"indexed": false, "name": "expirationTime", "type": "uint256"}, {"indexed": false, "name": "salt", "type": "uint256"}, {"indexed": false, "name": "orderbookInclusionDesired", "type": "bool"}], "name": "OrderApprovedPartTwo", "type": "event"}', 'OrderApprovedPartTwo') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0xe55393c778364e440d958b39ac1debd99dcfae3775a8a04d1e79124adf6a2d08" and selector_hash = abs(hash("0xe55393c778364e440d958b39ac1debd99dcfae3775a8a04d1e79124adf6a2d08")) % 10

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
        data.input.hash as hash, data.input.howtocall as howToCall, data.input.calldata as calldata, data.input.replacementpattern as replacementPattern, data.input.statictarget as staticTarget, data.input.staticextradata as staticExtradata, data.input.paymenttoken as paymentToken, data.input.baseprice as basePrice, data.input.extra as extra, data.input.listingtime as listingTime, data.input.expirationtime as expirationTime, data.input.salt as salt, data.input.orderbookinclusiondesired as orderbookInclusionDesired
    from base
)

select /*+ REPARTITION(50) */ *
from final
