{{
    config(
        materialized='table',
        file_format='parquet',
        alias='wyvernexchangev1_call_calculatematchprice_',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_calculatematchprice__calldecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV1_calculateMatchPrice__CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        opensea_wyvernexchangev1_calculatematchprice__calldecodeudf(unhex_input, unhex_output, '{"type": "function", "name": "calculateMatchPrice_", "constant": true, "payable": false, "stateMutability": "view", "inputs": [{"name": "addrs", "type": "address[14]"}, {"name": "uints", "type": "uint256[18]"}, {"name": "feeMethodsSidesKindsHowToCalls", "type": "uint8[8]"}, {"name": "calldataBuy", "type": "bytes"}, {"name": "calldataSell", "type": "bytes"}, {"name": "replacementPatternBuy", "type": "bytes"}, {"name": "replacementPatternSell", "type": "bytes"}, {"name": "staticExtradataBuy", "type": "bytes"}, {"name": "staticExtradataSell", "type": "bytes"}], "outputs": [{"name": "", "type": "uint256"}]}', 'calculateMatchPrice_') as data
    from {{ ref('stg_traces') }}
    where to_address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b") and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10 and selector = "0xd537e131" and selector_hash = abs(hash("0xd537e131")) % 10

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
        data.input.addrs as addrs, data.input.uints as uints, data.input.feemethodssideskindshowtocalls as feeMethodsSidesKindsHowToCalls, data.input.calldatabuy as calldataBuy, data.input.calldatasell as calldataSell, data.input.replacementpatternbuy as replacementPatternBuy, data.input.replacementpatternsell as replacementPatternSell, data.input.staticextradatabuy as staticExtradataBuy, data.input.staticextradatasell as staticExtradataSell, data.output.output_0 as output_0
    from base
)

select /*+ REPARTITION(50) */ *
from final
