{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameauctioncontroller_call_available',
        pre_hook={
            'sql': 'create or replace function ens_shortnameauctioncontroller_available_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameAuctionController_available_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_shortnameauctioncontroller_available_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "available", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "name", "type": "string"}], "outputs": [{"name": "", "type": "bool"}]}', 'available') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17") and address_hash = abs(hash(lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17"))) % 10 and selector = "0xaeb8ce9b" and selector_hash = abs(hash("0xaeb8ce9b")) % 10

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
        data.input.name as name, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
