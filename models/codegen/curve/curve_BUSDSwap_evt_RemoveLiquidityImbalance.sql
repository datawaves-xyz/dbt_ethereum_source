{{
    config(
        materialized='table',
        file_format='parquet',
        alias='busdswap_evt_removeliquidityimbalance',
        pre_hook={
            'sql': 'create or replace function curve_busdswap_removeliquidityimbalance_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_BUSDSwap_RemoveLiquidityImbalance_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_busdswap_removeliquidityimbalance_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "provider", "type": "address"}, {"indexed": false, "name": "token_amounts", "type": "uint256[4]"}, {"indexed": false, "name": "fees", "type": "uint256[4]"}, {"indexed": false, "name": "invariant", "type": "uint256"}, {"indexed": false, "name": "token_supply", "type": "uint256"}], "name": "RemoveLiquidityImbalance", "type": "event"}', 'RemoveLiquidityImbalance') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27") and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10 and selector = "0xb964b72f73f5ef5bf0fdc559b2fab9a7b12a39e47817a547f1f0aee47febd602" and selector_hash = abs(hash("0xb964b72f73f5ef5bf0fdc559b2fab9a7b12a39e47817a547f1f0aee47febd602")) % 10

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
