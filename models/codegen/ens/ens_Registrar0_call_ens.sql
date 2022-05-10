{{
    config(
        materialized='table',
        file_format='parquet',
        alias='registrar0_call_ens',
        pre_hook={
            'sql': 'create or replace function ens_registrar0_ens_calldecodeudf as "io.iftech.sparkudf.hive.Ens_Registrar0_ens_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_registrar0_ens_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [], "name": "ens", "outputs": [{"name": "", "type": "address"}], "payable": false, "type": "function"}', 'ens') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef") and address_hash = abs(hash(lower("0x6090A6e47849629b7245Dfa1Ca21D94cd15878Ef"))) % 10 and selector = "0x3f15457f" and selector_hash = abs(hash("0x3f15457f")) % 10

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
