{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_call_nonces',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_nonces_calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_nonces_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev2_nonces_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "nonces", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]}', 'nonces') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0x7ecebe00" and selector_hash = abs(hash("0x7ecebe00")) % 10

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
        data.input._0 as _0, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
