{{
    config(
        materialized='table',
        file_format='parquet',
        alias='ethregistrarcontroller2_evt_newpriceoracle',
        pre_hook={
            'sql': 'create or replace function ens_ethregistrarcontroller2_newpriceoracle_eventdecodeudf as "io.iftech.sparkudf.hive.Ens_ETHRegistrarController2_NewPriceOracle_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.13.jar";'
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
        ens_ethregistrarcontroller2_newpriceoracle_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "oracle", "type": "address", "internalType": "address"}], "name": "NewPriceOracle", "type": "event"}', 'NewPriceOracle') as data
    from {{ ref('stg_logs') }}
    where address = lower("0xB22c1C159d12461EA124b0deb4b5b93020E6Ad16") and address_hash = abs(hash(lower("0xB22c1C159d12461EA124b0deb4b5b93020E6Ad16"))) % 10 and selector = "0xf261845a790fe29bbd6631e2ca4a5bdc83e6eed7c3271d9590d97287e00e9123" and selector_hash = abs(hash("0xf261845a790fe29bbd6631e2ca4a5bdc83e6eed7c3271d9590d97287e00e9123")) % 10

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
