{{
    config(
        materialized='table',
        file_format='parquet',
        alias='cryptopunksmarket_call_punksofferedforsale',
        pre_hook={
            'sql': 'create or replace function cryptopunks_cryptopunksmarket_punksofferedforsale_calldecodeudf as "io.iftech.sparkudf.hive.Cryptopunks_CryptoPunksMarket_punksOfferedForSale_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        cryptopunks_cryptopunksmarket_punksofferedforsale_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "punksOfferedForSale", "constant": true, "payable": false, "inputs": [{"name": "", "type": "uint256"}], "outputs": [{"name": "isForSale", "type": "bool"}, {"name": "punkIndex", "type": "uint256"}, {"name": "seller", "type": "address"}, {"name": "minValue", "type": "uint256"}, {"name": "onlySellTo", "type": "address"}]}', 'punksOfferedForSale') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB") and address_hash = abs(hash(lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"))) % 10 and selector = "0x088f11f3" and selector_hash = abs(hash("0x088f11f3")) % 10

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
        data.input._0 as _0, data.output.output_isforsale as output_isForSale, data.output.output_punkindex as output_punkIndex, data.output.output_seller as output_seller, data.output.output_minvalue as output_minValue, data.output.output_onlysellto as output_onlySellTo
    from base
)

select /*+ REPARTITION(50) */ *
from final
