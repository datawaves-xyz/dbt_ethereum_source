{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_evt_claimsubmitted',
        pre_hook={
            'sql': 'create or replace function ens_shortnameclaims_claimsubmitted_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameClaims_ClaimSubmitted_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_shortnameclaims_claimsubmitted_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "claimed", "type": "string"}, {"indexed": false, "name": "dnsname", "type": "bytes"}, {"indexed": false, "name": "paid", "type": "uint256"}, {"indexed": false, "name": "claimant", "type": "address"}, {"indexed": false, "name": "email", "type": "string"}], "name": "ClaimSubmitted", "type": "event"}', 'ClaimSubmitted') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0x186f55cfb37bd38b311b8d5e8a212edf83c4d92107f48dbb7a4a5c217714eab1" and selector_hash = abs(hash("0x186f55cfb37bd38b311b8d5e8a212edf83c4d92107f48dbb7a4a5c217714eab1")) % 10

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
        data.input.claimed as claimed, data.input.dnsname as dnsname, data.input.paid as paid, data.input.claimant as claimant, data.input.email as email
    from base
)

select /*+ REPARTITION(50) */ *
from final
