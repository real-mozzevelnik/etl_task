--Для удобства восприятия сгенерированы ASCII таблицы с примерами выходных данных


--Топ самых продаваемых категорий за период среди выбранных категорий
WITH
    p as (
        SELECT
            category_name,
            product_id
        FROM products_categories FINAL
        WHERE category_name in ('Category_958', 'Category_971', 'Category_791', 'Category_950', 'Category_964', 'Category_857', 'Category_948')
    )
SELECT 
    p.category_name,
    count() AS total_sales_count
FROM sales AS s
INNER JOIN p ON s.product_id = p.product_id
WHERE s.date BETWEEN '2024-01-01' AND '2024-02-01' 
GROUP BY 1
ORDER BY 2 DESC;

-- +-----------------+-------------------+
-- | category_name   | total_sales_count |
-- +-----------------+-------------------+
-- | Electronics     | 15234             |
-- | Home Appliances | 13456             |
-- | Fashion         | 12678             |
-- | Beauty          | 9876              |
-- | Sports          | 8756              |
-- | Toys            | 8234              |
-- | Books           | 7890              |
-- | Automotive      | 7345              |
-- | Grocery         | 6890              |
-- | Furniture       | 6543              |
-- +-----------------+-------------------+


--Топ категорий по рекламным вложениям за период среди выбранных категорий
WITH
    p as (
        SELECT
            category_name,
            product_id
        FROM products_categories FINAL
        WHERE category_name in ('Category_958', 'Category_971', 'Category_791', 'Category_950', 'Category_964', 'Category_857', 'Category_948')
    )
SELECT 
    p.category_name,
    sum(advertising_amount) AS advertising_amount_sum
FROM ads AS a
INNER JOIN p ON a.product_id = p.product_id
WHERE a.date BETWEEN '2024-01-01' AND '2024-02-01' 
GROUP BY 1
ORDER BY 2 DESC;

-- +-----------------+------------------------+
-- | category_name   | advertising_amount_sum |
-- +-----------------+------------------------+
-- | Electronics     | 5000                   |
-- | Fashion         | 4500                   |
-- | Home Appliances | 4200                   |
-- | Beauty          | 3500                   |
-- | Sports          | 3000                   |
-- | Toys            | 2500                   |
-- | Books           | 2000                   |
-- | Automotive      | 1800                   |
-- | Grocery         | 1500                   |
-- | Furniture       | 1200                   |
-- +-----------------+------------------------+


--Динамика по дням суммы проданных товаров, кол-ва проданных товаров, 
--рекламных расходов и коэффициента конверсии рекламы в заказы за период среди выбранных категорий
WITH
    p as (
        SELECT
            product_id
        FROM products_categories FINAL
        WHERE category_name in ('Category_958', 'Category_971', 'Category_791', 'Category_950', 'Category_964', 'Category_857', 'Category_948')
    ),
    a as (
        SELECT 
            date,
            sum(advertising_amount) as advertising_amount_sum
        FROM ads
        WHERE date BETWEEN '2024-01-01' AND '2024-01-15' 
        AND product_id in p
        GROUP BY date
    )
SELECT 
    date,
    sum(sales_amount) as sales_sum,
    count() as products_sold,
    any(advertising_amount_sum) as advertising_amount_sum,
    products_sold / advertising_amount_sum as ads_to_cart_conversion_coefficient
FROM sales as s
INNER JOIN a ON s.date = a.date
WHERE s.product_id in p
AND date BETWEEN '2024-01-01' AND '2024-01-15' 
GROUP BY 1
ORDER BY 1;

-- +------------+-----------+---------------+---------------------+-------------------------------------+
-- | date       | sales_sum | products_sold | advertising_amount  | ads_to_cart_conversion_coefficient  |
-- +------------+-----------+---------------+---------------------+-------------------------------------+
-- | 2024-01-01 | 12000     | 100           | 5000                | 0.02                                |
-- | 2024-01-02 | 13000     | 110           | 5200                | 0.021                               |
-- | 2024-01-03 | 12500     | 95            | 4800                | 0.019                               |
-- | 2024-01-04 | 14000     | 105           | 5100                | 0.021                               |
-- | 2024-01-05 | 13500     | 120           | 5300                | 0.023                               |
-- | 2024-01-06 | 13800     | 115           | 5400                | 0.021                               |
-- | 2024-01-07 | 14200     | 125           | 5500                | 0.023                               |
-- +------------+-----------+---------------+---------------------+-------------------------------------+


--Коэффициент корреляции Пирсона между суммой продаж и рекламными расходами 
--по месяцам за период среди выбранных категорий
WITH
    p as (
        SELECT
            product_id
        FROM products_categories FINAL
        WHERE category_name in ('Category_958', 'Category_971', 'Category_791', 'Category_950', 'Category_964', 'Category_857', 'Category_948')
    ),
    a as (
        SELECT 
            date,
            sum(advertising_amount) as advertising_amount_sum
        FROM ads
        WHERE date BETWEEN '2024-01-01' AND '2024-06-01' 
        AND product_id in p
        GROUP BY date
    )
SELECT
    toStartOfMonth(date) as month,
    corr(toFloat32(sales), toFloat32(ads)) as correlation_coefficient
FROM (
    SELECT 
        date,
        sum(sales_amount) as sales, any(advertising_amount_sum) as ads
    FROM sales as s
    INNER JOIN a ON s.date = a.date
    WHERE s.product_id in p
    AND date BETWEEN '2024-01-01' AND '2024-06-01' 
    GROUP BY 1
)
GROUP BY 1
ORDER BY 1;

-- +---------------+-----------------------------+
-- | month         | correlation_coefficient     |
-- +---------------+-----------------------------+
-- | 2024-01-01    | 0.85                        |
-- | 2024-02-01    | 0.80                        |
-- | 2024-03-01    | 0.78                        |
-- | 2024-04-01    | 0.82                        |
-- | 2024-05-01    | 0.87                        |
-- | 2024-06-01    | 0.90                        |
-- +---------------+-----------------------------+


--Динамика по дням средней цены заказа, кол-ва заказов и рекламных расходов за период
--Не имеет выборки по категориям
WITH
    s AS (
        SELECT
            date,
            sum(sales_amount) AS sales_sum,
            count() AS orders_count
        FROM sales
        WHERE date BETWEEN '2024-01-01' AND '2024-01-15'
        GROUP BY date
    ),
    a AS (
        SELECT 
            date,
            sum(advertising_amount) AS advertising_amount_sum
        FROM ads
        WHERE date BETWEEN '2024-01-01' AND '2024-01-15'
        GROUP BY date
    )
SELECT 
    s.date,
    s.sales_sum / orders_count AS avg_order_price,
    s.orders_count AS orders_num,
    a.advertising_amount_sum
FROM s
LEFT JOIN a ON s.date = a.date
ORDER BY s.date;

-- +------------+----------------+------------+---------------------+
-- | date       | avg_order_price| orders_num | advertising_amount  |
-- +------------+----------------+------------+---------------------+
-- | 2024-01-01 | 125.50         | 100        | 5000                |
-- | 2024-01-02 | 130.20         | 110        | 5200                |
-- | 2024-01-03 | 128.80         | 95         | 4800                |
-- | 2024-01-04 | 140.50         | 105        | 5100                |
-- | 2024-01-05 | 135.00         | 120        | 5300                |
-- | 2024-01-06 | 138.90         | 115        | 5400                |
-- | 2024-01-07 | 142.00         | 125        | 5500                |
-- +------------+----------------+------------+---------------------+