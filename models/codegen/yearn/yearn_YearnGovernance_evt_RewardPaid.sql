{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_evt_rewardpaid',
        pre_hook={
            'sql': 'create or replace function yearn_yearngovernance_rewardpaid_eventdecodeudf as "io.iftech.sparkudf.hive.Yearn_YearnGovernance_RewardPaid_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        yearn_yearngovernance_rewardpaid_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address", "internalType": "address"}, {"indexed": false, "name": "reward", "type": "uint256", "internalType": "uint256"}], "name": "RewardPaid", "type": "event"}', 'RewardPaid') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E") and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10 and selector = "0xe2403640ba68fed3a2f88b7557551d1993f84b99bb10ff833f0cf8db0c5e0486" and selector_hash = abs(hash("0xe2403640ba68fed3a2f88b7557551d1993f84b99bb10ff833f0cf8db0c5e0486")) % 10

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
