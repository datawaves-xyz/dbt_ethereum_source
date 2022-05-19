{{
    config(
        materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['dt'],
        file_format='parquet',
        alias='erc721_evt_approval',
        pre_hook={
            'sql': 'create or replace function common_erc721_approval_eventdecodeudf as "io.iftech.sparkudf.hive.Common_ERC721_Approval_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        common_erc721_approval_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "owner", "type": "address", "internalType": "address"}, {"indexed": true, "name": "approved", "type": "address", "internalType": "address"}, {"indexed": true, "name": "tokenId", "type": "uint256", "internalType": "uint256"}], "name": "Approval", "type": "event"}', 'Approval') as data
    from {{ ref('stg_logs') }}
    where selector = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" and selector_hash = abs(hash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")) % 10

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
        data.input.owner as owner, data.input.approved as approved, data.input.tokenid as tokenId
    from base
)

select /*+ REPARTITION(dt) */ *
from final
