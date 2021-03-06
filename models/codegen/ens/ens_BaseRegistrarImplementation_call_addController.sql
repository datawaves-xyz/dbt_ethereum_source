{{
    config(
        materialized='table',
        file_format='parquet',
        alias='baseregistrarimplementation_call_addcontroller',
        pre_hook={
            'sql': 'create or replace function ens_baseregistrarimplementation_addcontroller_calldecodeudf as "io.iftech.sparkudf.hive.Ens_BaseRegistrarImplementation_addController_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_baseregistrarimplementation_addcontroller_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "addController", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "controller", "type": "address"}], "outputs": []}', 'addController') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85") and address_hash = abs(hash(lower("0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"))) % 10 and selector = "0xa7fc7a07" and selector_hash = abs(hash("0xa7fc7a07")) % 10

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
        data.input.controller as controller
    from base
)

select /*+ REPARTITION(50) */ *
from final
