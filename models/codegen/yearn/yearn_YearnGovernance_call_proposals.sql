{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_call_proposals',
        pre_hook={
            'sql': 'create or replace function yearn_yearngovernance_proposals_calldecodeudf as "io.iftech.sparkudf.hive.Yearn_YearnGovernance_proposals_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        yearn_yearngovernance_proposals_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "proposals", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "", "type": "uint256"}], "outputs": [{"name": "id", "type": "uint256"}, {"name": "proposer", "type": "address"}, {"name": "totalForVotes", "type": "uint256"}, {"name": "totalAgainstVotes", "type": "uint256"}, {"name": "start", "type": "uint256"}, {"name": "end", "type": "uint256"}]}', 'proposals') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E") and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10 and selector = "0x013cf08b" and selector_hash = abs(hash("0x013cf08b")) % 10

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
        data.input._0 as _0, data.output.output_id as output_id, data.output.output_proposer as output_proposer, data.output.output_totalforvotes as output_totalForVotes, data.output.output_totalagainstvotes as output_totalAgainstVotes, data.output.output_start as output_start, data.output.output_end as output_end
    from base
)

select /*+ REPARTITION(50) */ *
from final
