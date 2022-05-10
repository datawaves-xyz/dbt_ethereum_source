{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistrywithfallback_evt_approvalforall',
        pre_hook={
            'sql': 'create or replace function ens_ensregistrywithfallback_approvalforall_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistryWithFallback_ApprovalForAll_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_ensregistrywithfallback_approvalforall_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "owner", "type": "address"}, {"indexed": true, "internalType": "address", "name": "operator", "type": "address"}, {"indexed": false, "internalType": "bool", "name": "approved", "type": "bool"}], "name": "ApprovalForAll", "type": "event"}', 'ApprovalForAll') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31" and selector_hash = abs(hash("0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31")) % 10

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

select /*+ REPARTITION(50) */ *
from final
