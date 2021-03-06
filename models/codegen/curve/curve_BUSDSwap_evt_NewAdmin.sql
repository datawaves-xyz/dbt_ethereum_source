{{
    config(
        materialized='table',
        file_format='parquet',
        alias='busdswap_evt_newadmin',
        pre_hook={
            'sql': 'create or replace function curve_busdswap_newadmin_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_BUSDSwap_NewAdmin_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        curve_busdswap_newadmin_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "admin", "type": "address"}], "name": "NewAdmin", "type": "event"}', 'NewAdmin') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27") and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10 and selector = "0x71614071b88dee5e0b2ae578a9dd7b2ebbe9ae832ba419dc0242cd065a290b6c" and selector_hash = abs(hash("0x71614071b88dee5e0b2ae578a9dd7b2ebbe9ae832ba419dc0242cd065a290b6c")) % 10

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
        data.input.admin as admin
    from base
)

select /*+ REPARTITION(50) */ *
from final
