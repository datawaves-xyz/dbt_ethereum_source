{{
    config(
        materialized='table',
        file_format='parquet',
        alias='exchangestatev1_evt_operatoradded',
        pre_hook={
            'sql': 'create or replace function rariable_exchangestatev1_operatoradded_eventdecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeStateV1_OperatorAdded_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
        }
    )
}}

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        rariable_exchangestatev1_operatoradded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "account", "type": "address", "internalType": "address"}], "name": "OperatorAdded", "type": "event"}', 'OperatorAdded') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8") and address_hash = abs(hash(lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8"))) % 10 and selector = "0xac6fa858e9350a46cec16539926e0fde25b7629f84b5a72bffaae4df888ae86d" and selector_hash = abs(hash("0xac6fa858e9350a46cec16539926e0fde25b7629f84b5a72bffaae4df888ae86d")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        contract_address,
        dt,
        data.input.*
    from base
)

select /*+ REPARTITION(50) */ *
from final
