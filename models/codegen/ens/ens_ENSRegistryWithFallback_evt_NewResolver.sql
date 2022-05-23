{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistrywithfallback_evt_newresolver',
        pre_hook={
            'sql': 'create or replace function ens_ensregistrywithfallback_newresolver_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistryWithFallback_NewResolver_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_ensregistrywithfallback_newresolver_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32", "internalType": "bytes32"}, {"indexed": false, "name": "resolver", "type": "address", "internalType": "address"}], "name": "NewResolver", "type": "event"}', 'NewResolver') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0" and selector_hash = abs(hash("0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0")) % 10

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
