{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_evt_approvalforall',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_approvalforall_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_ApprovalForAll_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        ens_baseregistrarimplementation_approvalforall_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "owner", "type": "address", "internalType": "address"}, {"indexed": true, "name": "operator", "type": "address", "internalType": "address"}, {"indexed": false, "name": "approved", "type": "bool", "internalType": "bool"}], "name": "ApprovalForAll", "type": "event"}', 'ApprovalForAll') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" and selector_hash = abs(hash("0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31")) % 10

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
        data.input.owner as owner, data.input.operator as operator, data.input.approved as approved
    from base
)

select /*+ REPARTITION(50) */ *
from final
