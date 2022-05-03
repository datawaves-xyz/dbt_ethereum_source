{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_evt_addliquidity',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_addliquidity_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_AddLiquidity_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.3.jar";'
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
        curve_linkusdswap_addliquidity_eventdecodeudf(unhex_data, topics_arr, '{"name": "AddLiquidity", "inputs": [{"type": "address", "name": "provider", "indexed": true}, {"type": "uint256[2]", "name": "token_amounts", "indexed": false}, {"type": "uint256[2]", "name": "fees", "indexed": false}, {"type": "uint256", "name": "invariant", "indexed": false}, {"type": "uint256", "name": "token_supply", "indexed": false}], "anonymous": false, "type": "event"}', 'AddLiquidity') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171")
    and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10
    and selector = "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768"
    and selector_hash = abs(hash("0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768")) % 10

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

select /* REPARTITION(1) */ *
from final
