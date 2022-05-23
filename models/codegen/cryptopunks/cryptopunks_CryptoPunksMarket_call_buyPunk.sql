{{
    config(
        materialized='table',
        file_format='parquet',
        alias='cryptopunksmarket_call_buypunk',
        pre_hook={
            'sql': 'create or replace function cryptopunks_cryptopunksmarket_buypunk_calldecodeudf as "io.iftech.sparkudf.hive.Cryptopunks_CryptoPunksMarket_buyPunk_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        cryptopunks_cryptopunksmarket_buypunk_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "buyPunk", "constant": false, "payable": true, "inputs": [{"name": "punkIndex", "type": "uint256"}], "outputs": []}', 'buyPunk') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB") and address_hash = abs(hash(lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"))) % 10 and selector = "0x8264fe98" and selector_hash = abs(hash("0x8264fe98")) % 10

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
