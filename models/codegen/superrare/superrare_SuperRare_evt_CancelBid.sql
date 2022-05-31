{{
    config(
        materialized='table',
        file_format='parquet',
        alias='superrare_evt_cancelbid',
        pre_hook={
            'sql': 'create or replace function superrare_superrare_cancelbid_eventdecodeudf as "io.iftech.sparkudf.hive.Superrare_SuperRare_CancelBid_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.15.jar";'
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
        superrare_superrare_cancelbid_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "_bidder", "type": "address"}, {"indexed": true, "name": "_amount", "type": "uint256"}, {"indexed": true, "name": "_tokenId", "type": "uint256"}], "name": "CancelBid", "type": "event"}', 'CancelBid') as data
    from {{ ref('stg_logs') }}
    where address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d") and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10 and selector = "0x09dcebe16a733e22cc47e4959c50d4f21624d9f1815db32c2e439fbbd7b3eda0" and selector_hash = abs(hash("0x09dcebe16a733e22cc47e4959c50d4f21624d9f1815db32c2e439fbbd7b3eda0")) % 10

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
        data.input._bidder as _bidder, data.input._amount as _amount, data.input._tokenid as _tokenId
    from base
)

select /*+ REPARTITION(50) */ *
from final
