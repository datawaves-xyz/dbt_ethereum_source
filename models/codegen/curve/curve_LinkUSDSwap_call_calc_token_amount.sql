{{
    config(
        materialized='table',
        file_format='parquet',
        alias='linkusdswap_call_calc_token_amount',
        pre_hook={
            'sql': 'create or replace function curve_linkusdswap_calc_token_amount_calldecodeudf as "io.iftech.sparkudf.hive.Curve_LinkUSDSwap_calc_token_amount_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_linkusdswap_calc_token_amount_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "calc_token_amount", "stateMutability": "view", "inputs": [{"name": "amounts", "type": "uint256[2]"}, {"name": "is_deposit", "type": "bool"}], "outputs": [{"name": "", "type": "uint256"}]}', 'calc_token_amount') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171") and address_hash = abs(hash(lower("0xe7a24ef0c5e95ffb0f6684b813a78f2a3ad7d171"))) % 10 and selector = "0xed8e84f3" and selector_hash = abs(hash("0xed8e84f3")) % 10

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
