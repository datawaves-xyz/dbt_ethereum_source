{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_evt_newbid',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_newbid_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_NewBid_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_registrar0_newbid_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": true, "name": "bidder", "type": "address"}, {"indexed": false, "name": "deposit", "type": "uint256"}], "name": "NewBid", "type": "event"}', 'NewBid') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0xb556ff269c1b6714f432c36431e2041d28436a73b6c3f19c021827bbdc6bfc29" and selector_hash = abs(hash("0xb556ff269c1b6714f432c36431e2041d28436a73b6c3f19c021827bbdc6bfc29")) % 10

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
