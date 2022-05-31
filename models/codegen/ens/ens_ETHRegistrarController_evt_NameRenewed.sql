{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller_evt_namerenewed',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller_namerenewed_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController_NameRenewed_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.14.jar";'
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
        ens_ethregistrarcontroller_namerenewed_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "name", "type": "string"}, {"indexed": true, "name": "label", "type": "bytes32"}, {"indexed": false, "name": "cost", "type": "uint256"}, {"indexed": false, "name": "expires", "type": "uint256"}], "name": "NameRenewed", "type": "event"}', 'NameRenewed') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xF0AD5cAd05e10572EfcEB849f6Ff0c68f9700455") and address_hash = abs(hash(lower("0xF0AD5cAd05e10572EfcEB849f6Ff0c68f9700455"))) % 10 and selector = "0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae" and selector_hash = abs(hash("0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae")) % 10

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
        data.input.name as name, data.input.label as label, data.input.cost as cost, data.input.expires as expires
    from base
)

select /*+ REPARTITION(50) */ *
from final
