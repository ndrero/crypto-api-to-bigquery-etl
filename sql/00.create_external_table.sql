CREATE OR REPLACE EXTERNAL TABLE `@project_id.crypto_silver.ext_coingecko_coins_market`
WITH PARTITION COLUMNS (
    reference_dt DATE
)
OPTIONS (
    format='parquet',
    uris=['gs://crypto-prj-bucket/silver/crypto_market/*'],
    require_hive_partition_filter = false,
    hive_partition_uri_prefix = 'gs://crypto-prj-bucket/silver/crypto_market/'
)