use invest;-- using the invest database 
SELECT 
    *
FROM
    account_dim
LIMIT 100;-- to fasten the results limiting the result to 100 rows
SELECT 
    *
FROM
    customer_details
LIMIT 100;
SELECT 
    *
FROM
    holdings_current
LIMIT 100;
SELECT 
    *
FROM
    pricing_daily_new
LIMIT 100;
SELECT 
    *
FROM
    security_masterlist
LIMIT 100;
-- checking the portfolio information based on major and minor asset groups
SELECT 
    a.client_id,
    c1.full_name,
    s.ticker,
    s.major_asset_class,
    s.minor_asset_class
FROM
    security_masterlist AS s
        LEFT JOIN
    customer_details AS c1 ON s.row_names = c1.row_names
        LEFT JOIN
    account_dim AS a ON a.row_names = s.row_names
WHERE
    a.client_id IS NOT NULL
        AND full_name IS NOT NULL
ORDER BY s.major_asset_class , s.minor_asset_class;
-- calculating returns for the stocks 
SELECT a.date, a.ticker,
(a.value - a.lagged_price)/a.lagged_price as returns -- calculating the returns based on lag price
FROM 
(
SELECT *, LAG(value, 1) OVER(
						   PARTItiON BY ticker
                           ORDER BY date
                           ) AS lagged_price
 -- 1  according to time requiered
FROM pricing_daily_new
WHERE price_type = 'Adjusted'
LIMIT 5000
) a
;
-- creating a view to store the value for returns and risk calculation
create view invest.team_6_dual_degree2 as select a.date, a.ticker, a.value, a.lagged_price,
a.price_type, (a.value - a.lagged_price)/ a.lagged_price as returns
from
(select *, lag(value, 1) over(partition by ticker
order by date) as lagged_price
from invest.pricing_daily_new
where price_type = 'Adjusted' and date > '2019-09-09' ) a
;
-- calculating the mu, sigma and risk adj values for the portfolios
SELECT 
    ticker,
    AVG(returns) AS returns_value,
    STD(returns) AS total_risk,
    AVG(returns) / STD(returns) AS risk_adj_returns
FROM
    invest.team_6_dual_degree1
GROUP BY ticker;-- checking the risk and return based on the ticker values.
-- -------- checking the returns for the client 724 for 12,18,24 months

SELECT 
    A.ticker,
    E.security_name,
    E.sp500_weight,
    G.client_id,
    FORMAT((A.value - B.value) / B.value, 4) AS 12_Months_Return,
    FORMAT((A.value - C.value) / C.value, 4) AS 18_Months_Return,
    FORMAT((A.value - D.value) / D.value, 4) AS 24_Months_Return
FROM
    (SELECT 
        *
    FROM
        pricing_daily_new
    WHERE
        DATE = '2022-09-09'
            AND price_type = 'Adjusted') AS A
        LEFT JOIN
    pricing_daily_new AS B ON A.ticker = B.ticker
        AND B.DATE = DATE_SUB('2022-09-09', INTERVAL 365 DAY) -- for 12 months
        AND B.price_type = 'Adjusted'
        LEFT JOIN
    pricing_daily_new AS C ON A.ticker = C.ticker
        AND C.DATE = DATE_SUB('2022-09-09',
        INTERVAL 18 MONTH)
        AND C.price_type = 'Adjusted' -- 18 months
        LEFT JOIN
    pricing_daily_new AS D ON A.ticker = D.ticker
        AND D.DATE = DATE_SUB('2022-09-09',
        INTERVAL 24 MONTH)
        AND D.price_type = 'Adjusted' -- for 24 months and the price is filtered on adjusted price
        LEFT JOIN
    security_masterlist AS E ON A.ticker = E.ticker
        LEFT JOIN
    holdings_current AS F ON A.ticker = F.ticker
        LEFT JOIN
    account_dim AS G ON F.account_id = G.account_id
GROUP BY ticker -- grouping the results based on ticker
HAVING 12_Months_Return IS NOT NULL
    AND 18_Months_Return IS NOT NULL
    AND 24_Months_Return IS NOT NULL
    AND client_id = 724 -- filtering the client using having condition as group by is present
ORDER BY sp500_weight DESC
;
-- as we found out the indivisual custoner portfolios now we find out the returns of the market to align the goals of the customers
SELECT 
    (((SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2022-09-09'
                AND price_type = 'Adjusted') - (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2021-09-09'
                AND price_type = 'Adjusted'))) / (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2021-09-09'
                AND price_type = 'Adjusted') AS market_performance_12_months,
    (((SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2022-09-09'
                AND price_type = 'Adjusted') - (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2021-03-09'
                AND price_type = 'Adjusted'))) / (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2021-03-09'
                AND price_type = 'Adjusted') AS market_performance_18_months,
    (((SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2022-09-09'
                AND price_type = 'Adjusted') - (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2020-09-09'
                AND price_type = 'Adjusted'))) / (SELECT 
            (SUM(value))
        FROM
            pricing_daily_new
        WHERE
            date = '2020-09-09'
                AND price_type = 'Adjusted') AS market_performance_24_months
FROM
    pricing_daily_new
WHERE
    `value` IS NOT NULL
LIMIT 1; -- comparing the portfolio with market returns to give the recommendations
-- All Wells Fargo Average Portfolio Returns for the given time periods
use invest;
SELECT AVG((p1.value - p1.lagged_price12)/p1.lagged_price12) AS discrete_returns_12months,
		AVG(LN(p.value/p1.lagged_price12)) AS continuous_returns_12months, AVG((p1.value - p1.lagged_price18)/p1.lagged_price18) AS discrete_returns_18months, 
		AVG(LN(p.value/p1.lagged_price18)) AS continuous_returns_18months, AVG((p1.value - p1.lagged_price24)/p1.lagged_price24) AS discrete_returns_24months, 
		AVG(LN(p.value/p1.lagged_price24)) AS continuous_returns_24months
FROM
(SELECT *, LAG(value,250) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price_12months, 
LAG(value,375) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price_18months, 
LAG(value,500) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price_24months
FROM pricing_daily_new
WHERE price_type='Adjusted'
) AS p1
LEFT JOIN holdings_current AS hc
ON p1.ticker = hc.ticker
LEFT JOIN account_dim As a
ON a.account_id = hc.account_id
WHERE p1.date = '2022.09.09';
-- CLient 999 Average Portfolio Returns for the three period of times

SELECT AVG((p1.value - p1.lagged_price12)/p1.lagged_price12) AS discrete_returns_12months,
		AVG(LN(p1.value/p1.lagged_price12)) AS continuous_returns12, AVG((p1.value - p1.lagged_price18)/p1.lagged_price18) AS discrete_returns_18months, 
		AVG(LN(p1.value/p1.lagged_price18)) AS continuous_returns18, AVG((p1.value - p1.lagged_price24)/p1.lagged_price24) AS discrete_returns_24months, 
		AVG(LN(p1.value/p1.lagged_price24)) AS continuous_returns_24months
FROM
(SELECT *, LAG(value,250) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price12, 
LAG(value,375) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price18, 
LAG(value,500) OVER (PARTITION BY ticker ORDER BY date) AS Lagged_price24
FROM pricing_daily_new
WHERE price_type='Adjusted'
) AS p1
LEFT JOIN holdings_current AS hc
ON p1.ticker = hc.ticker
LEFT JOIN account_dim As a
ON a.account_id = hc.account_id
WHERE p1.date = '2022.09.09'
AND a.client_id = '999';
-- ---- checking the correlation of the market portfolio we use the holdings table for the calculation
SELECT 
    Pearson_Correlation_Coefficient,
    CASE
        WHEN Pearson_Correlation_Coefficient = 0 THEN 'There is No Correlation' -- when the coeff is 0 there is no corelation
        WHEN
            Pearson_Correlation_Coefficient > 0
                AND Pearson_Correlation_Coefficient < 0.3
        THEN
            'Low Correlation' -- using the case statement to identify different situations
       
        WHEN
            Pearson_Correlation_Coefficient >= 0.3
                AND Pearson_Correlation_Coefficient < 0.5
        THEN
            'Moderate Correlation'
       
        WHEN
            Pearson_Correlation_Coefficient >= 0.5
                AND Pearson_Correlation_Coefficient < 1.0
        THEN
            'High Correlation'
        
    END AS correlation_results -- optimising the correlation results
FROM
    (SELECT 
        ((AUM - (price * quantity / `count`)) / SQRT((price_square - POW(price, 2) / `count`) * (quantity_square - POW(quantity, 2) / `count`))) AS Pearson_Correlation_Coefficient
    FROM
        (SELECT 
        SUM(value) AS price,
            SUM(quantity) AS quantity,
            SUM(value * value) AS price_square,
            SUM(quantity * quantity) AS quantity_square,
            SUM(value * quantity) AS AUM,
            COUNT(*) AS `count`
    FROM
        holdings_current
    WHERE
        price_type = 'Adjusted') holdings_current
    GROUP BY AUM , price , quantity , `count` , price_square , quantity_square) holdings_current;
    --
    