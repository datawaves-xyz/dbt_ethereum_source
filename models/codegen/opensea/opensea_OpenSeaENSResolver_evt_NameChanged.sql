{{
    config(
        materialized='table',
        file_format='parquet',
        alias='openseaensresolver_evt_namechanged',
        pre_hook={
            'sql': 'create or replace function opensea_openseaensresolver_namechanged_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_OpenSeaENSResolver_NameChanged_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_openseaensresolver_namechanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": false, "name": "name", "type": "string"}], "name": "NameChanged", "type": "event"}', 'NameChanged') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956") and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10 and selector = "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7" and selector_hash = abs(hash("0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7")) % 10

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
        data.input.node as node, data.input.name as name
    from base
)

select /*+ REPARTITION(50) */ *
from final
