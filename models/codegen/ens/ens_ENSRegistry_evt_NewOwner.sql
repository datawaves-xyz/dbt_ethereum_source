{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistry_evt_newowner',
        pre_hook={
            'sql': 'create or replace function ens_ensregistry_newowner_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistry_NewOwner_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_ensregistry_newowner_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": true, "name": "label", "type": "bytes32"}, {"indexed": false, "name": "owner", "type": "address"}], "name": "NewOwner", "type": "event"}', 'NewOwner') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82" and selector_hash = abs(hash("0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82")) % 10

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
