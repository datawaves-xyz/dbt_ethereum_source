{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_evt_ownershiptransferred',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_ownershiptransferred_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_OwnershipTransferred_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_baseregistrarimplementation_ownershiptransferred_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "previousOwner", "type": "address", "internalType": "address"}, {"indexed": true, "name": "newOwner", "type": "address", "internalType": "address"}], "name": "OwnershipTransferred", "type": "event"}', 'OwnershipTransferred') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0" and selector_hash = abs(hash("0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0")) % 10

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
        data.input.previousowner as previousOwner, data.input.newowner as newOwner
    from base
)

select /*+ REPARTITION(50) */ *
from final
