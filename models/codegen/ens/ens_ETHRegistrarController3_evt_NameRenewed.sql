{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller3_evt_namerenewed',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller3_namerenewed_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController3_NameRenewed_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.11.jar";'
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
        ens_ethregistrarcontroller3_namerenewed_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "internalType": "string", "name": "name", "type": "string"}, {"indexed": true, "internalType": "bytes32", "name": "label", "type": "bytes32"}, {"indexed": false, "internalType": "uint256", "name": "cost", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "expires", "type": "uint256"}], "name": "NameRenewed", "type": "event"}', 'NameRenewed') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5") and address_hash = abs(hash(lower("0x283Af0B28c62C092C9727F1Ee09c02CA627EB7F5"))) % 10 and selector = "0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae" and selector_hash = abs(hash("0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae")) % 10

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
