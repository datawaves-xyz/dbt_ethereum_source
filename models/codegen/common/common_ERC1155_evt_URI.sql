{{
    config(
        materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt'],
        file_format='parquet',
        alias='erc1155_evt_uri',
        pre_hook={
            'sql': 'create or replace function common_erc1155_uri_eventdecodeudf as "io.iftech.sparkudf.hive.Common_ERC1155_URI_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        common_erc1155_uri_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "value", "type": "string", "internalType": "string"}, {"indexed": true, "name": "id", "type": "uint256", "internalType": "uint256"}], "name": "URI", "type": "event"}', 'URI') as data
    from {{ ref('stg_logs') }}
    where selector = "0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b" and selector_hash = abs(hash("0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b")) % 10

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
        data.input.value as value, data.input.id as id
    from base
)

select /*+ REPARTITION(dt) */ *
from final
