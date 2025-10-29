/*
Table: Customer

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| customer_id | int     |
| product_key | int     |
+-------------+---------+
This table may contain duplicates rows. 
customer_id is not NULL.
product_key is a foreign key (reference column) to Product table.
 

Table: Product

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| product_key | int     |
+-------------+---------+
product_key is the primary key (column with unique values) for this table.
 

Write a solution to report the customer ids from the Customer table that bought all the products in the Product table.

Return the result table in any order.

https://leetcode.com/problems/customers-who-bought-all-products/description/
*/

WITH counted_products AS (
    SELECT
        product_key,
        COUNT(product_key) OVER () products_cnt 
    FROM product_1045
)
SELECT DISTINCT customer_id
FROM (
    SELECT
        customer_id,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY c.product_key) bought_rn,
        products_cnt
    FROM customer_1045 c
    JOIN counted_products p ON p.product_key = c.product_key
) t
WHERE bought_rn = products_cnt;
