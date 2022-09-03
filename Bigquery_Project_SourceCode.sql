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


