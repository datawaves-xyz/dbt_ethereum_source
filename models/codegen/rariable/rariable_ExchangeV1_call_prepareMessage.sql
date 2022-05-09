{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangev1_call_preparemessage',
        pre_hook={
            'sql': 'create or replace function rariable_exchangev1_preparemessage_calldecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeV1_prepareMessage_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.10.jar";'
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
        rariable_exchangev1_preparemessage_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [{"components": [{"components": [{"internalType": "address", "name": "owner", "type": "address"}, {"internalType": "uint256", "name": "salt", "type": "uint256"}, {"components": [{"internalType": "address", "name": "token", "type": "address"}, {"internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"internalType": "enum ExchangeDomainV1.AssetType", "name": "assetType", "type": "uint8"}], "internalType": "struct ExchangeDomainV1.Asset", "name": "sellAsset", "type": "tuple"}, {"components": [{"internalType": "address", "name": "token", "type": "address"}, {"internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"internalType": "enum ExchangeDomainV1.AssetType", "name": "assetType", "type": "uint8"}], "internalType": "struct ExchangeDomainV1.Asset", "name": "buyAsset", "type": "tuple"}], "internalType": "struct ExchangeDomainV1.OrderKey", "name": "key", "type": "tuple"}, {"internalType": "uint256", "name": "selling", "type": "uint256"}, {"internalType": "uint256", "name": "buying", "type": "uint256"}, {"internalType": "uint256", "name": "sellerFee", "type": "uint256"}], "internalType": "struct ExchangeDomainV1.Order", "name": "order", "type": "tuple"}], "name": "prepareMessage", "outputs": [{"internalType": "string", "name": "", "type": "string"}], "payable": false, "stateMutability": "pure", "type": "function"}', 'prepareMessage') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06") and address_hash = abs(hash(lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06"))) % 10 and selector = "0x049944b6" and selector_hash = abs(hash("0x049944b6")) % 10

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
