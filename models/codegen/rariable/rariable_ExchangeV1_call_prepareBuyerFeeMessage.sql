{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangev1_call_preparebuyerfeemessage',
        pre_hook={
            'sql': 'create or replace function rariable_exchangev1_preparebuyerfeemessage_calldecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeV1_prepareBuyerFeeMessage_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        rariable_exchangev1_preparebuyerfeemessage_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "prepareBuyerFeeMessage", "constant": true, "payable": false, "stateMutability": "pure", "inputs": [{"name": "order", "type": "tuple", "components": [{"name": "key", "type": "tuple", "components": [{"name": "owner", "type": "address"}, {"name": "salt", "type": "uint256"}, {"name": "sellAsset", "type": "tuple", "components": [{"name": "token", "type": "address"}, {"name": "tokenId", "type": "uint256"}, {"name": "assetType", "type": "uint8"}]}, {"name": "buyAsset", "type": "tuple", "components": [{"name": "token", "type": "address"}, {"name": "tokenId", "type": "uint256"}, {"name": "assetType", "type": "uint8"}]}]}, {"name": "selling", "type": "uint256"}, {"name": "buying", "type": "uint256"}, {"name": "sellerFee", "type": "uint256"}]}, {"name": "fee", "type": "uint256"}], "outputs": [{"name": "", "type": "string"}]}', 'prepareBuyerFeeMessage') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06") and address_hash = abs(hash(lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06"))) % 10 and selector = "0x1b4c9874" and selector_hash = abs(hash("0x1b4c9874")) % 10

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
        data.input.order as order, data.input.fee as fee, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
