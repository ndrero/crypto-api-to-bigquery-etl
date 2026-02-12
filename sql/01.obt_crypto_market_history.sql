DECLARE table_exists BOOL;

SET table_exists = (
    SELECT COUNT(*) > 0
    FROM `@project_id.crypto_gold.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'obt_crypto_market'
);

IF NOT table_exists THEN
    CREATE TABLE `@project_id.crypto_gold.obt_crypto_market`
    PARTITION BY reference_dt
    OPTIONS (
        partition_expiration_days = 10)
    AS (
        SELECT 
            coin_id,
            current_price_usd,
            market_cap_usd,
            fully_diluted_valuation_usd,
            total_volume_usd,
            high_24h_usd,
            low_24h_usd,
            price_change_24h_usd,
            price_change_24h_pct,
            market_cap_change_24h_usd,
            market_cap_change_24h_pct,
            circulating_supply,
            total_supply,
            max_supply,
            all_time_high_usd,
            all_time_high_change_pct,
            all_time_high_date,
            all_time_low_usd,
            all_time_low_change_pct,
            all_time_low_date,
            return_on_investment_times,
            return_on_investment_currency,
            return_on_investment_pct,
            last_updated_at,
            ingested_at,
            reference_dt
        FROM `@project_id.crypto_silver.ext_coingecko_coins_market`
    );

ELSE
    MERGE `@project_id.crypto_gold.obt_crypto_market` mkt
    USING `@project_id.crypto_silver.ext_coingecko_coins_market` nw
    ON mkt.coin_id = nw.coin_id
    AND mkt.reference_dt = nw.reference_dt

    WHEN MATCHED THEN 
        UPDATE SET
            mkt.current_price_usd = nw.current_price_usd,
            mkt.market_cap_usd = nw.market_cap_usd,
            mkt.fully_diluted_valuation_usd = nw.fully_diluted_valuation_usd,
            mkt.total_volume_usd = nw.total_volume_usd,
            mkt.high_24h_usd = nw.high_24h_usd,
            mkt.low_24h_usd = nw.low_24h_usd,
            mkt.price_change_24h_usd = nw.price_change_24h_usd,
            mkt.price_change_24h_pct = nw.price_change_24h_pct,
            mkt.market_cap_change_24h_usd = nw.market_cap_change_24h_usd,
            mkt.market_cap_change_24h_pct = nw.market_cap_change_24h_pct,
            mkt.circulating_supply = nw.circulating_supply,
            mkt.total_supply = nw.total_supply,
            mkt.max_supply = nw.max_supply,
            mkt.all_time_high_usd = nw.all_time_high_usd,
            mkt.all_time_high_change_pct = nw.all_time_high_change_pct,
            mkt.all_time_high_date = nw.all_time_high_date,
            mkt.all_time_low_usd = nw.all_time_low_usd,
            mkt.all_time_low_change_pct = nw.all_time_low_change_pct,
            mkt.all_time_low_date = nw.all_time_low_date,
            mkt.return_on_investment_times = nw.return_on_investment_times,
            mkt.return_on_investment_currency = nw.return_on_investment_currency,
            mkt.return_on_investment_pct = nw.return_on_investment_pct,
            mkt.last_updated_at = nw.last_updated_at
        
    WHEN NOT MATCHED THEN
        INSERT (coin_id, current_price_usd, market_cap_usd, fully_diluted_valuation_usd, total_volume_usd, high_24h_usd, low_24h_usd, price_change_24h_usd, price_change_24h_pct, market_cap_change_24h_usd, market_cap_change_24h_pct, circulating_supply, total_supply, max_supply, all_time_high_usd, all_time_high_change_pct, all_time_high_date, all_time_low_usd, all_time_low_change_pct, all_time_low_date, return_on_investment_times, return_on_investment_currency, return_on_investment_pct, last_updated_at, ingested_at, reference_dt)
        VALUES (nw.coin_id, nw.current_price_usd, nw.market_cap_usd, nw.fully_diluted_valuation_usd, nw.total_volume_usd, nw.high_24h_usd, nw.low_24h_usd, nw.price_change_24h_usd, nw.price_change_24h_pct, nw.market_cap_change_24h_usd, nw.market_cap_change_24h_pct, nw.circulating_supply, nw.total_supply, nw.max_supply, nw.all_time_high_usd, nw.all_time_high_change_pct, nw.all_time_high_date, nw.all_time_low_usd, nw.all_time_low_change_pct, nw.all_time_low_date, nw.return_on_investment_times, nw.return_on_investment_currency, nw.return_on_investment_pct, nw.last_updated_at, nw.ingested_at, nw.reference_dt);

END IF;