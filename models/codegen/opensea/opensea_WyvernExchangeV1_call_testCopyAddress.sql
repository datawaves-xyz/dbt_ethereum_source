{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev1_call_testcopyaddress',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_testcopyaddress_calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV1_testCopyAddress_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev1_testcopyaddress_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "testCopyAddress", "constant": true, "payable": false, "stateMutability": "pure", "inputs": [{"name": "addr", "type": "address"}], "outputs": [{"name": "", "type": "bytes"}]}', 'testCopyAddress') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b") and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10 and selector = "0x3464af6a" and selector_hash = abs(hash("0x3464af6a")) % 10

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
        data.input.addr as addr, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
