{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev1_evt_ordercancelled',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_ordercancelled_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV1_OrderCancelled_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev1_ordercancelled_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}], "name": "OrderCancelled", "type": "event"}', 'OrderCancelled') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b") and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10 and selector = "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d" and selector_hash = abs(hash("0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d")) % 10

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
        data.input.hash as hash
    from base
)

select /*+ REPARTITION(50) */ *
from final
