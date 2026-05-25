WITH source AS (
    SELECT
        symbol,
        TRY_CAST(current_price AS DOUBLE) AS current_price_dbl,
        market_timestamp,
        CAST(market_timestamp AS DATE) AS trade_date
    FROM {{ ref('silver_clean_stock_quotes') }}
    WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),

latest_day_per_symbol AS (
    SELECT
        symbol,
        MAX(trade_date) AS max_trade_date
    FROM source
    GROUP BY symbol
),

latest_price AS (
    SELECT
        s.symbol,
        AVG(s.current_price_dbl) AS avg_price
    FROM source s
    JOIN latest_day_per_symbol ld
        ON s.symbol = ld.symbol
       AND s.trade_date = ld.max_trade_date
    GROUP BY s.symbol
),

all_time_volatility AS (
    SELECT
        symbol,
        STDDEV_POP(current_price_dbl) AS volatility,
        STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0) AS relative_volatility
    FROM source
    GROUP BY symbol
) 

SELECT
    lp.symbol,
    lp.avg_price,
    v.volatility,
    v.relative_volatility
FROM latest_price lp
JOIN all_time_volatility v
    ON lp.symbol = v.symbol
ORDER BY lp.symbol