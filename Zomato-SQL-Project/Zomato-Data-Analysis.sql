-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creata Database and Tables

-- COMMAND ----------

CREATE DATABASE zomato_hive

-- COMMAND ----------

USE DATABASE zomato_hive

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC def create_table(path):
-- MAGIC   df = spark.read.format("csv")\
-- MAGIC     .option("header", "true")\
-- MAGIC     .option("inferSchema", "true")\
-- MAGIC     .load(path)
-- MAGIC   
-- MAGIC   file_name = path.split("___")[-1].split(".")[0]
-- MAGIC   df.write.format("delta").saveAsTable(file_name)
-- MAGIC
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___delivery_partner.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___food.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___menu.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___order_details.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___orders.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___restaurants.csv")
-- MAGIC create_table("dbfs:/FileStore/zomato_schema___users.csv")
-- MAGIC

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Business Problems and Solution queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Find customers who have never ordered.

-- COMMAND ----------

select
  name
from
  users
where
  user_id not in (
    select
      distinct user_id
    from
      orders
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. What is the Average Price/dish

-- COMMAND ----------

select
  f.f_name,
  avg(price) as average_price
from
  food f
  join menu m on m.f_id = f.f_id
group by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Find the top restaurant in terms of the number of orders in June.

-- COMMAND ----------

select
  r.r_name as restaurant_name,
  count(r.r_id) as order_cnt
from
  orders o
  join restaurants r on r.r_id = o.r_id
where
  month(date) = '06'
group by
  1
order by
  2 desc
limit
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Restaurants with monthly sales greater than 1000 for July.

-- COMMAND ----------

-- DBTITLE 1,Method 1
with sales_view as (
  select
    r.r_name as restaurant_name,
    sum(o.amount) as sales
  from
    orders o
    join restaurants r on r.r_id = o.r_id
  where
    month(date) = '07'
  group by
    1
)
select
  *
from
  sales_view
where
  sales >= 1000
order by
  sales

-- COMMAND ----------

-- DBTITLE 1,Method 2
select
  r.r_name as restaurant_name,
  sum(o.amount) as sales
from
  orders o
  join restaurants r on r.r_id = o.r_id
where
  month(date) = '07'
group by
  1
having
  sales >= 1000
order by
  2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Show all orders with order details of Nitish (user_id = 1) from 10th June’22 to 10th July’22.

-- COMMAND ----------

select
  orders.date as order_date,
  users.name,
  restaurants.r_name,
  food.f_name
from
  orders
  join order_details on order_details.order_id = orders.order_id
  join users on users.user_id = orders.user_id
  join restaurants on restaurants.r_id = orders.r_id
  join food on food.f_id = order_details.f_id
where
  date between "2022-06-10"
  and "2022-07-10"
  and orders.user_id = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Find restaurants with maximum repeat customers.

-- COMMAND ----------

select
  r.r_name as restaurant_name,
  count(*) as loyal_customer,
  sum(order_cnt) as total_order_cnt
from
  (
    select
      o.r_id,
      o.user_id,
      count(o.order_id) as order_cnt
    from
      orders o
      join restaurants r on r.r_id = o.r_id
      join users u on u.user_id = o.user_id
    group by
      1,
      2
    having
      order_cnt > 1
  ) t
  join restaurants r on t.r_id = r.r_id
group by
  1
having
  loyal_customer > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Month-over-month revenue growth of Zomato.

-- COMMAND ----------

WITH monthly_revenue AS (
    SELECT 
        CASE 
            WHEN MONTH(date) = 5 THEN 'May'
            WHEN MONTH(date) = 6 THEN 'June'
            WHEN MONTH(date) = 7 THEN 'July'
        END AS month, 
        SUM(amount) AS revenue
    FROM orders
    WHERE MONTH(date) IN (5, 6, 7)
    GROUP BY 1
),
revenue_with_lag AS (
    SELECT 
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY 
            CASE 
                WHEN month = 'May' THEN 1
                WHEN month = 'June' THEN 2
                WHEN month = 'July' THEN 3
            END
        ) AS prev_revenue
    FROM monthly_revenue
)
SELECT 
    month, 
    revenue, 
    ((revenue - prev_revenue) / prev_revenue) * 100 AS MoM_Revenue
FROM revenue_with_lag;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. Customer — favorite food.

-- COMMAND ----------

WITH cust_food AS (
  SELECT
    u.name,
    f.f_name,
    COUNT(od.f_id) AS order_cnt,
    RANK() OVER (
      PARTITION BY u.name
      ORDER BY
        COUNT(od.f_id) DESC
    ) AS order_rank
  FROM
    order_details od
    JOIN orders o ON o.order_id = od.order_id
    JOIN food f ON f.f_id = od.f_id
    JOIN users u ON u.user_id = o.user_id
  GROUP BY
    1,
    2
)
SELECT
  name,
  f_name,
  order_cnt
FROM
  cust_food
WHERE
  order_rank = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 9. Find the most loyal customers for all restaurants.

-- COMMAND ----------

WITH loyalty AS (
  SELECT
    u.name as user_name,
    r.r_name as restaurant_name,
    COUNT(o.r_id) AS order_count,
    DENSE_RANK() OVER (
      PARTITION BY r.r_name
      ORDER BY
        COUNT(o.r_id) DESC
    ) AS row_rank
  FROM
    orders o
    JOIN restaurants r ON o.r_id = r.r_id
    JOIN users u ON o.user_id = u.user_id
  GROUP BY
    1,
    2
)
SELECT
  user_name,
  restaurant_name,
  order_count
FROM
  loyalty
WHERE
  row_rank = 1
order by
  restaurant_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10. Month-over-month revenue growth of each restaurant.

-- COMMAND ----------

WITH monthly_revenue AS (
  SELECT
    restaurants.r_name as restaurant_name,
    CASE
      WHEN MONTH(date) = 5 THEN 'May'
      WHEN MONTH(date) = 6 THEN 'June'
      WHEN MONTH(date) = 7 THEN 'July'
    END AS month,
    SUM(orders.amount) AS revenue,
    orders.r_id as rest_id
  FROM
    orders
    join restaurants on restaurants.r_id = orders.r_id
  WHERE
    MONTH(date) IN (5, 6, 7)
  GROUP BY
    1,
    2,
    4
),
revenue_with_lag AS (
  SELECT
    restaurant_name,
    rest_id,
    month,
    revenue,
    LAG(revenue) OVER (
      PARTITION BY restaurant_name
      ORDER BY
        CASE
          WHEN month = 'May' THEN 1
          WHEN month = 'June' THEN 2
          WHEN month = 'July' THEN 3
        END
    ) AS prev_revenue
  FROM
    monthly_revenue
)
SELECT
  restaurant_name,
  month,
  revenue,
  (
    (revenue - prev_revenue) / NULLIF (prev_revenue, 0)
  ) * 100 AS MoM_Revenue
FROM
  revenue_with_lag
ORDER BY
  rest_id;
