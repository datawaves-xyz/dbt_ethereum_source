{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_evt_nameregistered',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_nameregistered_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_NameRegistered_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        ens_baseregistrarimplementation_nameregistered_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "id", "type": "uint256", "internalType": "uint256"}, {"indexed": true, "name": "owner", "type": "address", "internalType": "address"}, {"indexed": false, "name": "expires", "type": "uint256", "internalType": "uint256"}], "name": "NameRegistered", "type": "event"}', 'NameRegistered') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9" and selector_hash = abs(hash("0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9")) % 10

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
        data.input.id as id, data.input.owner as owner, data.input.expires as expires
    from base
)

select /*+ REPARTITION(50) */ *
from final
