{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_call_submitprefixclaim',
        pre_hook={
            'sql': 'create or replace function ens_shortnameclaims_submitprefixclaim_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameClaims_submitPrefixClaim_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
        }
    )
}}

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        ens_shortnameclaims_submitprefixclaim_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "submitPrefixClaim", "constant": false, "payable": true, "stateMutability": "payable", "inputs": [{"name": "name", "type": "bytes"}, {"name": "claimant", "type": "address"}, {"name": "email", "type": "string"}], "outputs": []}', 'submitPrefixClaim') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0x4d4de844" and selector_hash = abs(hash("0x4d4de844")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        call_success,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        dt,
        data.input.name as name, data.input.claimant as claimant, data.input.email as email
    from base
)

select /*+ REPARTITION(50) */ *
from final
