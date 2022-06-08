{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_call_hashorder_',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_hashorder__calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_hashOrder__CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev2_hashorder__calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "hashOrder_", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "addrs", "type": "address[7]"}, {"name": "uints", "type": "uint256[9]"}, {"name": "feeMethod", "type": "uint8"}, {"name": "side", "type": "uint8"}, {"name": "saleKind", "type": "uint8"}, {"name": "howToCall", "type": "uint8"}, {"name": "calldata", "type": "bytes"}, {"name": "replacementPattern", "type": "bytes"}, {"name": "staticExtradata", "type": "bytes"}], "outputs": [{"name": "", "type": "bytes32"}]}', 'hashOrder_') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0x71d02b38" and selector_hash = abs(hash("0x71d02b38")) % 10

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
        data.input.addrs as addrs, data.input.uints as uints, data.input.feemethod as feeMethod, data.input.side as side, data.input.salekind as saleKind, data.input.howtocall as howToCall, data.input.calldata as calldata, data.input.replacementpattern as replacementPattern, data.input.staticextradata as staticExtradata, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
