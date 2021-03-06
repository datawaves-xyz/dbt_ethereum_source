{{
    config(
        materialized='table',
        file_format='parquet',
        alias='yearngovernance_evt_rewardadded',
        pre_hook={
            'sql': 'create or replace function yearn_yearngovernance_rewardadded_eventdecodeudf as "io.iftech.sparkudf.hive.Yearn_YearnGovernance_RewardAdded_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.16.jar";'
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
        yearn_yearngovernance_rewardadded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "reward", "type": "uint256", "internalType": "uint256"}], "name": "RewardAdded", "type": "event"}', 'RewardAdded') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E") and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10 and selector = "0xde88a922e0d3b88b24e9623efeb464919c6bf9f66857a65e2bfcf2ce87a9433d" and selector_hash = abs(hash("0xde88a922e0d3b88b24e9623efeb464919c6bf9f66857a65e2bfcf2ce87a9433d")) % 10

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
        data.input.reward as reward
    from base
)

select /*+ REPARTITION(50) */ *
from final
