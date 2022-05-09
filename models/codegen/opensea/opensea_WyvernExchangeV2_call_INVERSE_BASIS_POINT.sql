{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev2_call_inverse_basis_point',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev2_inverse_basis_point_calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV2_INVERSE_BASIS_POINT_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.9.jar";'
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
        opensea_wyvernexchangev2_inverse_basis_point_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [], "name": "INVERSE_BASIS_POINT", "outputs": [{"name": "", "type": "uint256"}], "payable": false, "stateMutability": "view", "type": "function"}', 'INVERSE_BASIS_POINT') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5") and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10 and selector = "0xcae6047f" and selector_hash = abs(hash("0xcae6047f")) % 10

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
