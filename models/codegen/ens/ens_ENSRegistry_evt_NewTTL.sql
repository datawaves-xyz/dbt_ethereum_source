{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ensregistry_evt_newttl',
        pre_hook={
            'sql': 'create or replace function ens_ensregistry_newttl_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ENSRegistry_NewTTL_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_ensregistry_newttl_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": false, "name": "ttl", "type": "uint64"}], "name": "NewTTL", "type": "event"}', 'NewTTL') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x314159265dd8dbb310642f98f50c066173c1259b") and address_hash = abs(hash(lower("0x314159265dd8dbb310642f98f50c066173c1259b"))) % 10 and selector = "0x1d4f9bbfc9cab89d66e1a1562f2233ccbf1308cb4f63de2ead5787adddb8fa68" and selector_hash = abs(hash("0x1d4f9bbfc9cab89d66e1a1562f2233ccbf1308cb4f63de2ead5787adddb8fa68")) % 10

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
