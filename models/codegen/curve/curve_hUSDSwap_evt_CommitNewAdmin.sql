{{
    config(
        materialized='table',
        file_format='parquet',
        alias='husdswap_evt_commitnewadmin',
        pre_hook={
            'sql': 'create or replace function curve_husdswap_commitnewadmin_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_hUSDSwap_CommitNewAdmin_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        curve_husdswap_commitnewadmin_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "deadline", "type": "uint256"}, {"indexed": true, "name": "admin", "type": "address"}], "name": "CommitNewAdmin", "type": "event"}', 'CommitNewAdmin') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604") and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10 and selector = "0x181aa3aa17d4cbf99265dd4443eba009433d3cde79d60164fde1d1a192beb935" and selector_hash = abs(hash("0x181aa3aa17d4cbf99265dd4443eba009433d3cde79d60164fde1d1a192beb935")) % 10

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
