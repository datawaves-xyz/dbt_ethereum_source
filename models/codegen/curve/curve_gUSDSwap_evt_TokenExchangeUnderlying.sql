{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_evt_tokenexchangeunderlying',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_tokenexchangeunderlying_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_TokenExchangeUnderlying_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        curve_gusdswap_tokenexchangeunderlying_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "buyer", "type": "address"}, {"indexed": false, "name": "sold_id", "type": "int128"}, {"indexed": false, "name": "tokens_sold", "type": "uint256"}, {"indexed": false, "name": "bought_id", "type": "int128"}, {"indexed": false, "name": "tokens_bought", "type": "uint256"}], "name": "TokenExchangeUnderlying", "type": "event"}', 'TokenExchangeUnderlying') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b" and selector_hash = abs(hash("0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b")) % 10

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
        data.input.buyer as buyer, data.input.sold_id as sold_id, data.input.tokens_sold as tokens_sold, data.input.bought_id as bought_id, data.input.tokens_bought as tokens_bought
    from base
)

select /*+ REPARTITION(50) */ *
from final
