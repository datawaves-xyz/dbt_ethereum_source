{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_evt_hashinvalidated',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_hashinvalidated_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_HashInvalidated_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        ens_registrar0_hashinvalidated_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": true, "name": "name", "type": "string"}, {"indexed": false, "name": "value", "type": "uint256"}, {"indexed": false, "name": "registrationDate", "type": "uint256"}], "name": "HashInvalidated", "type": "event"}', 'HashInvalidated') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0x1f9c649fe47e58bb60f4e52f0d90e4c47a526c9f90c5113df842c025970b66ad" and selector_hash = abs(hash("0x1f9c649fe47e58bb60f4e52f0d90e4c47a526c9f90c5113df842c025970b66ad")) % 10

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
        data.input.hash as hash, data.input.name as name, data.input.value as value, data.input.registrationdate as registrationDate
    from base
)

select /*+ REPARTITION(50) */ *
from final
