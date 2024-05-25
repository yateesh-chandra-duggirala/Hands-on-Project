## Problem Statement: 
- Tiny shop is a small restaurant. 
- They want a brief analysis of their sales, product performance, orders, and sales trends. 
- So, I write some SQL queries to find some insights from the given data.

There are four tables. These are

1. Customers: In this table, all the restaurant’s customer data is stored. Data like customer ID, customer first name, customer last name, and email ID are in this table.
2. Products: Here are details of all the products that are sold in the restaurant. Product ID, product name, and price are in this table. From this, we can see which is the most and least expensive product.
3. Orders: Here, all the order data is stored which were placed by customers. In this table, the order ID, customer ID, and order date columns are included.
4. Order Items: This table contains the order ID, Product ID and quantity ordered.

To do the analysis I used Delta Lake over the Databricks Workspace. 