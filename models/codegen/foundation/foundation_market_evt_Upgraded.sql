{{
    config(
        materialized='table',
        file_format='parquet',
        alias='market_evt_upgraded',
        pre_hook={
            'sql': 'create or replace function foundation_market_upgraded_eventdecodeudf as "io.iftech.sparkudf.hive.Foundation_market_Upgraded_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        foundation_market_upgraded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "implementation", "type": "address", "internalType": "address"}], "name": "Upgraded", "type": "event"}', 'Upgraded') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f") and address_hash = abs(hash(lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f"))) % 10 and selector = "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b" and selector_hash = abs(hash("0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b")) % 10

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
