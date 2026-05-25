SELECT
    symbol,
    current_price,
    change_amount,
    change_percent
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY fetched_at DESC) flag_last
    FROM {{ ref('silver_clean_stock_quotes') }}) t
WHERE flag_last = 1