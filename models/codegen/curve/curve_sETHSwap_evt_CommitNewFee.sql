{{
    config(
        materialized='table',
        file_format='parquet',
        alias='sethswap_evt_commitnewfee',
        pre_hook={
            'sql': 'create or replace function curve_sethswap_commitnewfee_eventdecodeudf as "io.iftech.sparkudf.hive.Curve_sETHSwap_CommitNewFee_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        curve_sethswap_commitnewfee_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "deadline", "type": "uint256"}, {"indexed": false, "name": "fee", "type": "uint256"}, {"indexed": false, "name": "admin_fee", "type": "uint256"}], "name": "CommitNewFee", "type": "event"}', 'CommitNewFee') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xc5424b857f758e906013f3555dad202e4bdb4567") and address_hash = abs(hash(lower("0xc5424b857f758e906013f3555dad202e4bdb4567"))) % 10 and selector = "0x351fc5da2fbf480f2225debf3664a4bc90fa9923743aad58b4603f648e931fe0" and selector_hash = abs(hash("0x351fc5da2fbf480f2225debf3664a4bc90fa9923743aad58b4603f648e931fe0")) % 10

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
        data.input.deadline as deadline, data.input.fee as fee, data.input.admin_fee as admin_fee
    from base
)

select /*+ REPARTITION(50) */ *
from final
