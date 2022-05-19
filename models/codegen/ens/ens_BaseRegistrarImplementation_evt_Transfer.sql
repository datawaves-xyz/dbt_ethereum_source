{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_evt_transfer',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_transfer_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_Transfer_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        ens_baseregistrarimplementation_transfer_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "from", "type": "address", "internalType": "address"}, {"indexed": true, "name": "to", "type": "address", "internalType": "address"}, {"indexed": true, "name": "tokenId", "type": "uint256", "internalType": "uint256"}], "name": "Transfer", "type": "event"}', 'Transfer') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and selector_hash = abs(hash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")) % 10

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
        data.input.from as from, data.input.to as to, data.input.tokenid as tokenId
    from base
)

select /*+ REPARTITION(50) */ *
from final
