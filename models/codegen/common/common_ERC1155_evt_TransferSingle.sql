{{
    config(
        materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt'],
        file_format='parquet',
        alias='erc1155_evt_transfersingle',
        pre_hook={
            'sql': 'create or replace function common_erc1155_transfersingle_eventdecodeudf as "io.iftech.sparkudf.hive.Common_ERC1155_TransferSingle_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.8.jar";'
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
        common_erc1155_transfersingle_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "operator", "type": "address"}, {"indexed": true, "internalType": "address", "name": "from", "type": "address"}, {"indexed": true, "internalType": "address", "name": "to", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "id", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "value", "type": "uint256"}], "name": "TransferSingle", "type": "event"}', 'TransferSingle') as data
    from {{ ref('stg_logs') }}
    where selector = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62" and selector_hash = abs(hash("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62")) % 10

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

select /*+ REPARTITION(dt) */ *
from final
