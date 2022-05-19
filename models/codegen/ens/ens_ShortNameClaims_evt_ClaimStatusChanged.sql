{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_evt_claimstatuschanged',
        pre_hook={
            'sql': 'create or replace function ens_shortnameclaims_claimstatuschanged_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameClaims_ClaimStatusChanged_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        ens_shortnameclaims_claimstatuschanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "claimId", "type": "bytes32"}, {"indexed": false, "name": "status", "type": "uint8"}], "name": "ClaimStatusChanged", "type": "event"}', 'ClaimStatusChanged') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0x698a3a7f5c0bc9915a6f167e9ce03ffc660392e807cf6fd57fd9ae52063dd27b" and selector_hash = abs(hash("0x698a3a7f5c0bc9915a6f167e9ce03ffc660392e807cf6fd57fd9ae52063dd27b")) % 10

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
        data.input.claimid as claimId, data.input.status as status
    from base
)

select /*+ REPARTITION(50) */ *
from final
