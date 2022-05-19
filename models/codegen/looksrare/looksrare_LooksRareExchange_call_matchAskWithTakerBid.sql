{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_call_matchaskwithtakerbid',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_matchaskwithtakerbid_calldecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_matchAskWithTakerBid_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.12.jar";'
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
        looksrare_looksrareexchange_matchaskwithtakerbid_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "matchAskWithTakerBid", "stateMutability": "nonpayable", "inputs": [{"name": "takerBid", "type": "tuple", "components": [{"name": "isOrderAsk", "type": "bool"}, {"name": "taker", "type": "address"}, {"name": "price", "type": "uint256"}, {"name": "tokenId", "type": "uint256"}, {"name": "minPercentageToAsk", "type": "uint256"}, {"name": "params", "type": "bytes"}]}, {"name": "makerAsk", "type": "tuple", "components": [{"name": "isOrderAsk", "type": "bool"}, {"name": "signer", "type": "address"}, {"name": "collection", "type": "address"}, {"name": "price", "type": "uint256"}, {"name": "tokenId", "type": "uint256"}, {"name": "amount", "type": "uint256"}, {"name": "strategy", "type": "address"}, {"name": "currency", "type": "address"}, {"name": "nonce", "type": "uint256"}, {"name": "startTime", "type": "uint256"}, {"name": "endTime", "type": "uint256"}, {"name": "minPercentageToAsk", "type": "uint256"}, {"name": "params", "type": "bytes"}, {"name": "v", "type": "uint8"}, {"name": "r", "type": "bytes32"}, {"name": "s", "type": "bytes32"}]}], "outputs": []}', 'matchAskWithTakerBid') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0x38e29209" and selector_hash = abs(hash("0x38e29209")) % 10

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
        data.input.takerbid as takerBid, data.input.makerask as makerAsk
    from base
)

select /*+ REPARTITION(50) */ *
from final
