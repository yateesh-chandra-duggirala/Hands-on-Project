-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Database and Tables

-- COMMAND ----------

CREATE DATABASE tiny_shop_sales

-- COMMAND ----------

USE DATABASE tiny_shop_sales

-- COMMAND ----------

-- DBTITLE 1,Customers Table
CREATE TABLE customers (
    customer_id integer,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(100)
);

INSERT INTO customers (customer_id, first_name, last_name, email) VALUES
(1, 'John', 'Doe', 'johndoe@email.com'),
(2, 'Jane', 'Smith', 'janesmith@email.com'),
(3, 'Bob', 'Johnson', 'bobjohnson@email.com'),
(4, 'Alice', 'Brown', 'alicebrown@email.com'),
(5, 'Charlie', 'Davis', 'charliedavis@email.com'),
(6, 'Eva', 'Fisher', 'evafisher@email.com'),
(7, 'George', 'Harris', 'georgeharris@email.com'),
(8, 'Ivy', 'Jones', 'ivyjones@email.com'),
(9, 'Kevin', 'Miller', 'kevinmiller@email.com'),
(10, 'Lily', 'Nelson', 'lilynelson@email.com'),
(11, 'Oliver', 'Patterson', 'oliverpatterson@email.com'),
(12, 'Quinn', 'Roberts', 'quinnroberts@email.com'),
(13, 'Sophia', 'Thomas', 'sophiathomas@email.com');

-- COMMAND ----------

-- DBTITLE 1,Products Table

CREATE TABLE products (
    product_id integer,
    product_name varchar(100),
    price decimal
);

INSERT INTO products (product_id, product_name, price) VALUES
(1, 'Product A', 10.00),
(2, 'Product B', 15.00),
(3, 'Product C', 20.00),
(4, 'Product D', 25.00),
(5, 'Product E', 30.00),
(6, 'Product F', 35.00),
(7, 'Product G', 40.00),
(8, 'Product H', 45.00),
(9, 'Product I', 50.00),
(10, 'Product J', 55.00),
(11, 'Product K', 60.00),
(12, 'Product L', 65.00),
(13, 'Product M', 70.00);

-- COMMAND ----------

-- DBTITLE 1,Orders Table
CREATE TABLE orders (
    order_id integer,
    customer_id integer,
    order_date date
);

INSERT INTO orders (order_id, customer_id, order_date) VALUES
(1, 1, '2023-05-01'),
(2, 2, '2023-05-02'),
(3, 3, '2023-05-03'),
(4, 1, '2023-05-04'),
(5, 2, '2023-05-05'),
(6, 3, '2023-05-06'),
(7, 4, '2023-05-07'),
(8, 5, '2023-05-08'),
(9, 6, '2023-05-09'),
(10, 7, '2023-05-10'),
(11, 8, '2023-05-11'),
(12, 9, '2023-05-12'),
(13, 10, '2023-05-13'),
(14, 11, '2023-05-14'),
(15, 12, '2023-05-15'),
(16, 13, '2023-05-16');


-- COMMAND ----------

-- DBTITLE 1,order_items
CREATE TABLE order_items (
    order_id integer,
    product_id integer,
    quantity integer
);

INSERT INTO order_items (order_id, product_id, quantity) VALUES
(1, 1, 2),
(1, 2, 1),
(2, 2, 1),
(2, 3, 3),
(3, 1, 1),
(3, 3, 2),
(4, 2, 4),
(4, 3, 1),
(5, 1, 1),
(5, 3, 2),
(6, 2, 3),
(6, 1, 1),
(7, 4, 1),
(7, 5, 2),
(8, 6, 3),
(8, 7, 1),
(9, 8, 2),
(9, 9, 1),
(10, 10, 3),
(10, 11, 2),
(11, 12, 1),
(11, 13, 3),
(12, 4, 2),
(12, 5, 1),
(13, 6, 3),
(13, 7, 2),
(14, 8, 1),
(14, 9, 2),
(15, 10, 3),
(15, 11, 1),
(16, 12, 2),
(16, 13, 3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Which product has the highest price? Only return a single row.

-- COMMAND ----------

select
  product_name
from
  products
where
  price = (
    select
      max(price)
    from
      products
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Which customer has made the most orders?

-- COMMAND ----------

with order_cust as (
  select
    concat(c.first_name, ' ', c.last_name) as full_name,
    count(o.customer_id) as cnt
  from
    customers c
    inner join orders o on c.customer_id = o.customer_id
  group by
    1
)
select
  *
from
  order_cust
where
  cnt = (
    select
      max(cnt)
    from
      order_cust
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. What's the total revenue per product?

-- COMMAND ----------

select
  p.product_name,
  sum(p.price * o.quantity) as revenue
from
  products p
  inner join order_items o on p.product_id = o.product_id
group by
  1
order by
  2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Find the orders and revenue trend

-- COMMAND ----------

select
  o.order_date,
  count(o.order_id) as orders,
  sum(p.price * oi.quantity) as revenue
from
  order_items oi
  join orders o on oi.order_id = o.order_id
  join products p ON oi.product_id = p.product_id
group by
  1
order by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Find the first order (by date) for each customer

-- COMMAND ----------

with first_order as (
  select
    concat(first_name, ' ', last_name) as customer_name,
    product_name
  from
    customers c
    join orders o on c.customer_id = o.customer_id
    join order_items oi on oi.order_id = o.order_id
    join products p on p.product_id = oi.product_id
  group by
    1,
    2,
    o.order_date
  having
    o.order_date = min(o.order_date)
)
select
  customer_name,
  collect_set(product_name) as first_product
from
  first_order
group by
  1
order by
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Find the top 3 customers who have ordered the most distinct products

-- COMMAND ----------

with customer as (
  select
    concat(first_name, ' ', last_name) as customer_name,
    count(distinct(products.product_id)) as distinct_products
  from
    customers
    join orders on customers.customer_id = orders.customer_id
    join order_items on orders.order_id = order_items.order_id
    join products on order_items.product_id = products.product_id
  group by
    1
  order by
    2 desc,
    customer_name
)
select
  *
from
  customer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Which product has been bought the highest & least in terms of quantity?

-- COMMAND ----------

with product_quant as (
  select
    product_name,
    sum(quantity) as total_qty
  from
    products
    join order_items on products.product_id = order_items.product_id
  group by
    1
),
x as (
  SELECT
    product_name,
    DENSE_RANK() OVER(
      ORDER BY
        total_qty DESC
    ) AS rnk1,
    DENSE_RANK() OVER(
      ORDER BY
        total_qty ASC
    ) AS rnk2
  FROM
    product_quant
)
select
  collect_list(
    CASE
      WHEN rnk1 = 1 THEN product_name
      ELSE NULL
    END
  ) AS most_ordered_product,
  collect_list(
    CASE
      WHEN rnk2 = 1 THEN product_name
      ELSE NULL
    END
  ) AS least_ordered_product
FROM
  x;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. What is the median order total?

-- COMMAND ----------

with x as (
  select
    quantity * price as orders
  from
    products
    join order_items on products.product_id = order_items.product_id
),
y as (
  select
    orders,
    row_number() over (
      order by
        orders
    ) as r1,
    count(*) over() as cnt
  from
    x
)
select
  avg(orders) as median_sales
from
  y
where
  r1 BETWEEN cnt / 2
  and (cnt / 2) + 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 9. Daywise Repeated Customer and New Customer

-- COMMAND ----------

WITH customer AS (
  SELECT
    c.customer_id,
    order_date,
    MIN(order_date) OVER(PARTITION BY c.customer_id) AS first_order_date
  FROM
    customers c
    JOIN orders o ON c.customer_id = o.customer_id
)
SELECT
  order_date,
  count(
    DISTINCT(
      CASE
        WHEN order_date = first_order_date THEN customer_id
        ELSE NULL
      END
    )
  ) as new_customers,
  count(
    DISTINCT(
      CASE
        WHEN order_date > first_order_date THEN customer_id
        ELSE NULL
      END
    )
  ) as repeated_customers
FROM
  customer
GROUP BY
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10. Products that make 70% of total revenue

-- COMMAND ----------

WITH x AS (
  SELECT
    p.product_id,
    product_name,
    SUM(quantity * price) AS revenue
  FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
  GROUP BY
    p.product_id,
    product_name
),
y AS (
  SELECT
    product_id,
    product_name,
    revenue,
    SUM(revenue) OVER(
      ORDER BY
        revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS running_sum,
    SUM(revenue) OVER() AS total_revenue
  FROM
    x
)
SELECT
  product_id,
  product_name,
  revenue
from
  y
where
  running_sum < 0.7 * total_revenue
