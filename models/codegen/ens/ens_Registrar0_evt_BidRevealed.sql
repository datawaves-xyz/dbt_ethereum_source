{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_evt_bidrevealed',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_bidrevealed_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_BidRevealed_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        ens_registrar0_bidrevealed_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": true, "name": "owner", "type": "address"}, {"indexed": false, "name": "value", "type": "uint256"}, {"indexed": false, "name": "status", "type": "uint8"}], "name": "BidRevealed", "type": "event"}', 'BidRevealed') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0x7b6c4b278d165a6b33958f8ea5dfb00c8c9d4d0acf1985bef5d10786898bc3e7" and selector_hash = abs(hash("0x7b6c4b278d165a6b33958f8ea5dfb00c8c9d4d0acf1985bef5d10786898bc3e7")) % 10

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
        data.input.hash as hash, data.input.owner as owner, data.input.value as value, data.input.status as status
    from base
)

select /*+ REPARTITION(50) */ *
from final
