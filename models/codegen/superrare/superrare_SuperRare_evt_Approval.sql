{{
    config(
        materialized='table',
        file_format='parquet',
        alias='superrare_evt_approval',
        pre_hook={
            'sql': 'create or replace function superrare_superrare_approval_eventdecodeudf as "io.iftech.sparkudf.hive.Superrare_SuperRare_Approval_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        superrare_superrare_approval_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "_owner", "type": "address"}, {"indexed": true, "name": "_approved", "type": "address"}, {"indexed": false, "name": "_tokenId", "type": "uint256"}], "name": "Approval", "type": "event"}', 'Approval') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d") and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10 and selector = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925" and selector_hash = abs(hash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")) % 10

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
        data.input._owner as _owner, data.input._approved as _approved, data.input._tokenid as _tokenId
    from base
)

select /*+ REPARTITION(50) */ *
from final
