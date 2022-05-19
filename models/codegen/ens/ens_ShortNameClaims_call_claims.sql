{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_call_claims',
        pre_hook={
            'sql': 'create or replace function ens_shortnameclaims_claims_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameClaims_claims_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        ens_shortnameclaims_claims_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "claims", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "", "type": "bytes32"}], "outputs": [{"name": "labelHash", "type": "bytes32"}, {"name": "claimant", "type": "address"}, {"name": "paid", "type": "uint256"}, {"name": "status", "type": "uint8"}]}', 'claims') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0xeff0f592" and selector_hash = abs(hash("0xeff0f592")) % 10

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
        data.input._0 as _0, data.output.output_labelhash as output_labelHash, data.output.output_claimant as output_claimant, data.output.output_paid as output_paid, data.output.output_status as output_status
    from base
)

select /*+ REPARTITION(50) */ *
from final
