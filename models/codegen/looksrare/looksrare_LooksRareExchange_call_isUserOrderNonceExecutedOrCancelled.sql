{{
    config(
        materialized='table',
        file_format='parquet',
        alias='looksrareexchange_call_isuserordernonceexecutedorcancelled',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_isuserordernonceexecutedorcancelled_calldecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_isUserOrderNonceExecutedOrCancelled_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        looksrare_looksrareexchange_isuserordernonceexecutedorcancelled_calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "isUserOrderNonceExecutedOrCancelled", "stateMutability": "view", "inputs": [{"name": "user", "type": "address"}, {"name": "orderNonce", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}]}', 'isUserOrderNonceExecutedOrCancelled') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a") and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10 and selector = "0x31e27e27" and selector_hash = abs(hash("0x31e27e27")) % 10

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
        data.input.user as user, data.input.ordernonce as orderNonce, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
