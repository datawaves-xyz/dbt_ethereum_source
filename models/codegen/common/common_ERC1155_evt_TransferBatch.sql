{{
    config(
        materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt'],
        file_format='parquet',
        alias='erc1155_evt_transferbatch',
        pre_hook={
            'sql': 'create or replace function common_erc1155_transferbatch_eventdecodeudf as "io.iftech.sparkudf.hive.Common_ERC1155_TransferBatch_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        common_erc1155_transferbatch_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "operator", "type": "address", "internalType": "address"}, {"indexed": true, "name": "from", "type": "address", "internalType": "address"}, {"indexed": true, "name": "to", "type": "address", "internalType": "address"}, {"indexed": false, "name": "ids", "type": "uint256[]", "internalType": "uint256[]"}, {"indexed": false, "name": "values", "type": "uint256[]", "internalType": "uint256[]"}], "name": "TransferBatch", "type": "event"}', 'TransferBatch') as data
    from {{ ref('stg_logs') }}
    where selector = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb" and selector_hash = abs(hash("0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb")) % 10

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
        data.input.operator as operator, data.input.from as from, data.input.to as to, data.input.ids as ids, data.input.values as values
    from base
)

select /*+ REPARTITION(dt) */ *
from final
