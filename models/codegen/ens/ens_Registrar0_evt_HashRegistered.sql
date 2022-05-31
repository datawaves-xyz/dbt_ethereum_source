{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_evt_hashregistered',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_hashregistered_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_HashRegistered_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_registrar0_hashregistered_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": true, "name": "owner", "type": "address"}, {"indexed": false, "name": "value", "type": "uint256"}, {"indexed": false, "name": "registrationDate", "type": "uint256"}], "name": "HashRegistered", "type": "event"}', 'HashRegistered') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0x0f0c27adfd84b60b6f456b0e87cdccb1e5fb9603991588d87fa99f5b6b61e670" and selector_hash = abs(hash("0x0f0c27adfd84b60b6f456b0e87cdccb1e5fb9603991588d87fa99f5b6b61e670")) % 10

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
        data.input.hash as hash, data.input.owner as owner, data.input.value as value, data.input.registrationdate as registrationDate
    from base
)

select /*+ REPARTITION(50) */ *
from final
