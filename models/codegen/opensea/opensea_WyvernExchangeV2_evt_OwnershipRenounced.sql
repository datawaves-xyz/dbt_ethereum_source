{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_evt_ownershiprenounced',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_ownershiprenounced_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_OwnershipRenounced_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev2_ownershiprenounced_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "previousOwner", "type": "address"}], "name": "OwnershipRenounced", "type": "event"}', 'OwnershipRenounced') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0xf8df31144d9c2f0f6b59d69b8b98abd5459d07f2742c4df920b25aae33c64820" and selector_hash = abs(hash("0xf8df31144d9c2f0f6b59d69b8b98abd5459d07f2742c4df920b25aae33c64820")) % 10

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
        data.input.previousowner as previousOwner
    from base
)

select /*+ REPARTITION(50) */ *
from final
