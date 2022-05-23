{{
    config(
        materialized='table',
        file_format='parquet',
        alias='gusdswap_evt_addliquidity',
        pre_hook={
            'sql': 'create or replace function curve_gusdswap_addliquidity_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_gUSDSwap_AddLiquidity_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_gusdswap_addliquidity_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "provider", "type": "address"}, {"indexed": false, "name": "token_amounts", "type": "uint256[2]"}, {"indexed": false, "name": "fees", "type": "uint256[2]"}, {"indexed": false, "name": "invariant", "type": "uint256"}, {"indexed": false, "name": "token_supply", "type": "uint256"}], "name": "AddLiquidity", "type": "event"}', 'AddLiquidity') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956") and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10 and selector = "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768" and selector_hash = abs(hash("0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768")) % 10

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
