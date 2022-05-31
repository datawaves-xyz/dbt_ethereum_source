{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_call_entries',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_entries_calldecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_entries_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_registrar0_entries_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "entries", "constant": true, "payable": false, "inputs": [{"name": "_hash", "type": "bytes32"}], "outputs": [{"name": "", "type": "uint8"}, {"name": "", "type": "address"}, {"name": "", "type": "uint256"}, {"name": "", "type": "uint256"}, {"name": "", "type": "uint256"}]}', 'entries') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0x267b6922" and selector_hash = abs(hash("0x267b6922")) % 10

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
        data.input._hash as _hash, data.output.output_0 as output_0, data.output.output_1 as output_1, data.output.output_2 as output_2, data.output.output_3 as output_3, data.output.output_4 as output_4
    from base
)

select /*+ REPARTITION(50) */ *
from final
