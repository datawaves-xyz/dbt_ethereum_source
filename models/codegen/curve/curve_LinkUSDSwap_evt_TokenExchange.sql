{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_evt_tokenexchange',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_tokenexchange_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_TokenExchange_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.3.jar";'
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
        curve_linkusdswap_tokenexchange_eventdecodeudf(unhex_data, topics_arr, '{"name": "TokenExchange", "inputs": [{"type": "address", "name": "buyer", "indexed": true}, {"type": "int128", "name": "sold_id", "indexed": false}, {"type": "uint256", "name": "tokens_sold", "indexed": false}, {"type": "int128", "name": "bought_id", "indexed": false}, {"type": "uint256", "name": "tokens_bought", "indexed": false}], "anonymous": false, "type": "event"}', 'TokenExchange') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171")
    and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10
    and selector = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
    and selector_hash = abs(hash("0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140")) % 10

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