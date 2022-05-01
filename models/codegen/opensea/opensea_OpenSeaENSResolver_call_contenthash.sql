{{
    config(
        materialized='table',
        file_format='parquet',
        alias='openseaensresolver_call_contenthash',
        pre_hook={
            'sql': 'create or replace function opensea_openseaensresolver_contenthash_calldecodeudf as "io.iftech.sparkudf.hive.Opensea_OpenSeaENSResolver_contenthash_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        opensea_openseaensresolver_contenthash_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [{"name": "node", "type": "bytes32"}], "name": "contenthash", "outputs": [{"name": "", "type": "bytes"}], "payable": false, "stateMutability": "view", "type": "function"}', 'contenthash') as data
    from {{ ref('stg_ethereum__traces') }}
    where to_address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956")
    and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10
    and selector = "0x30786263"
    and selector_hash = abs(hash("0x30786263")) % 10

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

select /* REPARTITION(dt) */ *
from final
