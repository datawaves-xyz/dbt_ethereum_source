{{
    config(
        materialized='table',
        file_format='parquet',
        alias='superrare_call_tokenuri',
        pre_hook={
            'sql': 'create or replace function superrare_superrare_tokenuri_calldecodeudf as "io.iftech.sparkudf.hive.Superrare_SuperRare_tokenURI_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        superrare_superrare_tokenuri_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "tokenURI", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "_tokenId", "type": "uint256"}], "outputs": [{"name": "", "type": "string"}]}', 'tokenURI') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d") and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10 and selector = "0xc87b56dd" and selector_hash = abs(hash("0xc87b56dd")) % 10

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
        data.input._tokenid as _tokenId, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
