{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_call_cancelorder_',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_cancelorder__calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_cancelOrder__CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.7.jar";'
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
        opensea_wyvernexchangev2_cancelorder__calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"name": "addrs", "type": "address[7]"}, {"name": "uints", "type": "uint256[9]"}, {"name": "feeMethod", "type": "uint8"}, {"name": "side", "type": "uint8"}, {"name": "saleKind", "type": "uint8"}, {"name": "howToCall", "type": "uint8"}, {"name": "calldata", "type": "bytes"}, {"name": "replacementPattern", "type": "bytes"}, {"name": "staticExtradata", "type": "bytes"}, {"name": "v", "type": "uint8"}, {"name": "r", "type": "bytes32"}, {"name": "s", "type": "bytes32"}], "name": "cancelOrder_", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'cancelOrder_') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0xa8a41c70" and selector_hash = abs(hash("0xa8a41c70")) % 10

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
        data.input.*,
        data.output.*
    from base
)

select /*+ REPARTITION(50) */ *
from final
