{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_evt_removeliquidityimbalance',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_removeliquidityimbalance_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_RemoveLiquidityImbalance_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_sethswap_removeliquidityimbalance_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "provider", "type": "address"}, {"indexed": false, "name": "token_amounts", "type": "uint256[2]"}, {"indexed": false, "name": "fees", "type": "uint256[2]"}, {"indexed": false, "name": "invariant", "type": "uint256"}, {"indexed": false, "name": "token_supply", "type": "uint256"}], "name": "RemoveLiquidityImbalance", "type": "event"}', 'RemoveLiquidityImbalance') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0x2b5508378d7e19e0d5fa338419034731416c4f5b219a10379956f764317fd47e" and selector_hash = abs(hash("0x2b5508378d7e19e0d5fa338419034731416c4f5b219a10379956f764317fd47e")) % 10

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
        data.input.provider as provider, data.input.token_amounts as token_amounts, data.input.fees as fees, data.input.invariant as invariant, data.input.token_supply as token_supply
    from base
)

select /*+ REPARTITION(50) */ *
from final
