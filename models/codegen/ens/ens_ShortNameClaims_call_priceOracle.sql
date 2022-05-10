{{
    config(
        materialized='table',
        file_format='parquet',
        alias='shortnameclaims_call_priceoracle',
        pre_hook={
            'sql': 'create or replace function ens_shortnameclaims_priceoracle_calldecodeudf as "io.iftech.sparkudf.hive.Ens_ShortNameClaims_priceOracle_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_shortnameclaims_priceoracle_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [], "name": "priceOracle", "outputs": [{"name": "", "type": "address"}], "payable": false, "stateMutability": "view", "type": "function"}', 'priceOracle') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749") and address_hash = abs(hash(lower("0xf7c83bd0c50e7a72b55a39fe0dabf5e3a330d749"))) % 10 and selector = "0x2630c12f" and selector_hash = abs(hash("0x2630c12f")) % 10

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
