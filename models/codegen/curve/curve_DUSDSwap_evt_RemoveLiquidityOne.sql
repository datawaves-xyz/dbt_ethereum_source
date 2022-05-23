{{
    config(
        materialized='table',
        file_format='parquet',
        alias='dusdswap_evt_removeliquidityone',
        pre_hook={
            'sql': 'create or replace function curve_dusdswap_removeliquidityone_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_DUSDSwap_RemoveLiquidityOne_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_dusdswap_removeliquidityone_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "provider", "type": "address"}, {"indexed": false, "name": "token_amount", "type": "uint256"}, {"indexed": false, "name": "coin_amount", "type": "uint256"}, {"indexed": false, "name": "token_supply", "type": "uint256"}], "name": "RemoveLiquidityOne", "type": "event"}', 'RemoveLiquidityOne') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c") and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10 and selector = "0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0" and selector_hash = abs(hash("0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0")) % 10

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
