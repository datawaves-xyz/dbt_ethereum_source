{{
    config(
        materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt'],
        file_format='parquet',
        alias='erc1155_evt_approvalforall',
        pre_hook={
            'sql': 'create or replace function common_erc1155_approvalforall_eventdecodeudf as "io.iftech.sparkudf.hive.Common_ERC1155_ApprovalForAll_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.10.jar";'
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
        common_erc1155_approvalforall_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "account", "type": "address"}, {"indexed": true, "internalType": "address", "name": "operator", "type": "address"}, {"indexed": false, "internalType": "bool", "name": "approved", "type": "bool"}], "name": "ApprovalForAll", "type": "event"}', 'ApprovalForAll') as data
    from {{ ref('stg_logs') }}
    where selector = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" and selector_hash = abs(hash("0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31")) % 10

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
