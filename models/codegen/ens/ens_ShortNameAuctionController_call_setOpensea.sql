{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameauctioncontroller_call_setopensea',
        pre_hook={
            'sql': 'create or replace function ens_shortnameauctioncontroller_setopensea_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameAuctionController_setOpensea_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        ens_shortnameauctioncontroller_setopensea_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "setOpensea", "constant": false, "payable": false, "stateMutability": "nonpayable", "inputs": [{"name": "_opensea", "type": "address"}], "outputs": []}', 'setOpensea') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17") and address_hash = abs(hash(lower("0x699C7F511C9e2182e89f29b3Bfb68bD327919D17"))) % 10 and selector = "0xb776c8a6" and selector_hash = abs(hash("0xb776c8a6")) % 10

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
        data.input._opensea as _opensea
    from base
)

select /*+ REPARTITION(50) */ *
from final
