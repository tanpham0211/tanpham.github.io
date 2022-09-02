-- Big project for SQL

--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Thụt dòng cho từng đoạn, từng phần để dễ nhìn hơn
--Mình k nên xử lý date bằng những hàm đc dùng để xử lý chuỗi như left, substring, concat
--vì lúc này data của mình vẫn ở dạng string, chứ k phải dạng date, khi xuất ra excel hay gg sheet thì phải xử lý thêm 1 bước nữa
--k nên đặt tên CTE là cte hoặc ABC,nên đặt tên viết tắt, mà nhìn vào mình có thể hiểu đc CTE đó đang lấy data gì

--nếu e biết mình chỉ cần lấy data trong T7 thì ghi FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
--và k cần ghi _table_suffix nữa luôn

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month




-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month



-- Query 06: Average amount of money spent per session
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions is not null
group by month


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce
#standardSQL
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and hits.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc

--
with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month


Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data



--------------------------------

-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
    LEFT(date,6) AS month
    ,SUM(totals.visits) AS visits
    ,SUM(totals.pageviews) AS pageviews
    ,SUM(totals.transactions) AS transaction
    ,SUM(totals.transactionRevenue) AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month



-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
    trafficSource.source
    ,SUM(totals.visits) AS total_visits
    ,SUM(totals.bounces) AS total_no_of_bounces
    ,(SAFE_DIVIDE(SUM(totals.bounces),SUM(totals.visits)) * 100) AS bounce_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
-- revenue of month
SELECT
  'Month' AS time_type,
  LEFT(date,6) AS time,
  trafficSource.source,
  SUM(totals.totalTransactionRevenue) AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY
  time_type,
  LEFT(date,6),
  trafficSource.source

  UNION ALL

-- revenue of week
SELECT
  "Week" AS time_type,
  FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) AS time,
  trafficSource.source,
  SUM(totals.totalTransactionRevenue) AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY
  time_type,
  time,
  trafficSource.source
ORDER BY revenue DESC

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH temp_purchaser AS (
SELECT
    LEFT(date,6) AS time,
    SAFE_DIVIDE(SUM(totals.pageviews),COUNT(DISTINCT fullVisitorId)) AS avg_pageviews_purchase
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170601' AND '20170731'
    AND totals.transactions >= 1
GROUP BY time
)

, temp_non_purchaser AS (
SELECT
    LEFT(date,6) AS time,
    SAFE_DIVIDE(SUM(totals.pageviews),COUNT(DISTINCT fullVisitorId)) AS avg_pageviews_non_purchase
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170601' AND '20170731'
    AND totals.transactions IS NULL
GROUP BY time
)

SELECT
    t1.time,
    t1.avg_pageviews_purchase,
    t2.avg_pageviews_non_purchase
FROM temp_purchaser AS t1
INNER JOIN temp_non_purchaser AS t2 ON t1.time=t2.time
ORDER BY t1.time



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
    LEFT(date,6) AS Month,
    SAFE_DIVIDE(SUM(totals.transactions),COUNT(DISTINCT fullVisitorId)) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
GROUP BY Month

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    LEFT(date,6) AS Month,
    SAFE_DIVIDE(SUM(totals.totalTransactionRevenue),SUM(totals.visits)) AS avg_revenue_by_user_per_visit
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY Month


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
WITH temp_user AS (
SELECT DISTINCT
    fullVisitorId
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` AS t
    ,UNNEST(hits) AS hits
    ,UNNEST(hits.product) AS p
WHERE p.productRevenue IS NOT NULL
    AND p.v2ProductName LIKE "YouTube Men's Vintage Henley"
)

SELECT
    p.v2ProductName AS other_purchased_products,
    SUM(p.productQuantity) AS quantity
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` AS t
    ,UNNEST(hits) AS hits
    ,UNNEST(hits.product) AS p
WHERE fullVisitorId IN (SELECT fullVisitorId FROM temp_user)
    AND p.v2ProductName NOT LIKE "YouTube Men's Vintage Henley"
    AND p.productRevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
SELECT
  LEFT(date,6) AS month,
  COUNTIF( hits.eCommerceAction.action_type = "2" ) AS num_product_view,
  COUNTIF( hits.eCommerceAction.action_type = "3" ) AS num_addtocart,
  COUNTIF( hits.eCommerceAction.action_type = "6" ) AS num_purchase,
  ROUND(SAFE_DIVIDE(COUNTIF( hits.eCommerceAction.action_type = "3" ),
    COUNTIF( hits.eCommerceAction.action_type = "2" )) * 100,2) AS add_to_cart_rate,
  ROUND(SAFE_DIVIDE(COUNTIF( hits.eCommerceAction.action_type = "6" ),
    COUNTIF( hits.eCommerceAction.action_type = "2" )) * 100,2) AS purchase_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS t,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS p
WHERE
  _table_suffix BETWEEN '20170101'AND '20170331'
GROUP BY
  month
ORDER BY
  month



-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    count(fullVisitorId) as visits,
    sum(totals.pageviews) as views,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/power(10, 6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by month
order by month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.bounces) as total_no_of_bounces,
    (sum(totals.bounces)/sum(totals.visits)) as bounce_rate
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017
with revenue_by_month as(
    select
        'Month' as time_type,
        format_date("%Y%m",parse_date("%Y%m%d",date)) as time,
        trafficSource.source as source,
        (sum(totals.totalTransactionRevenue)/power(10, 6)) as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by source, time
    ),

revenue_by_week as(
    select
        'Week' as time_type,
        format_date("%Y%W",parse_date("%Y%m%d",date)) as time,
        trafficSource.source as source,
        (sum(totals.totalTransactionRevenue)/power(10, 6)) as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by source, time
    )

select *
from revenue_by_month
union all
select *
from revenue_by_week
order by revenue desc;



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchaser_data as(
    select
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
    from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    where _table_suffix between '0601' and '0731'
    and totals.transactions>=1
    group by month
    ),

non_purchaser_data as(
    select
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
    from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    where _table_suffix between '0601' and '0731'
    and totals.transactions is null
    group by month
    )

select
    *
from purchaser_data
left join non_purchaser_data
    on purchaser_data.month = non_purchaser_data.month
order by purchaser_data.month;



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month;



-- Query 06: Average amount of money spent per session
#standardSQL
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions is not null
group by month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce
#standardSQL
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as h,
                        unnest(h.product) as p
                        where p.v2productname = "YouTube Men's Vintage Henley"
                        and h.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc;


with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with view_table as(
    select
        format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
        count(*) as num_product_view
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest(hits) as hits
    where hits.eCommerceAction.action_type = '2'
    and _table_suffix between '20170101' and '20170331'
    group by month
),

addtocard_table as(
    select
        format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
        count(*) as num_addtocard
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest(hits) as hits
    where hits.eCommerceAction.action_type = '3'
    and _table_suffix between '20170101' and '20170331'
    group by month
),

purchase_table as(
    select
        format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
        count(*) as num_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '6'
    and _table_suffix between '20170101' and '20170331'
    group by month
)

select
    *,
    (a.num_addtocard/v.num_product_view)*100 as addtocard_rate,
    (p.num_purchase/v.num_product_view)*100 as purchase_rate
from view_table as v
left join addtocard_table as a using(month)
left join purchase_table as p using(month)
order by v.month;
