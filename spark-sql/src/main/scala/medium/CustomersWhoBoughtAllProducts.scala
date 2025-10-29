package medium

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import utils.Utils.sparkReadPGTable

object CustomersWhoBoughtAllProducts extends App {
  /**
   * Table: Customer
   *
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | customer_id | int     |
   * | product_key | int     |
   * +-------------+---------+
   * This table may contain duplicates rows.
   * customer_id is not NULL.
   * product_key is a foreign key (reference column) to Product table.
   *
   * Table: Product
   *
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | product_key | int     |
   * +-------------+---------+
   * product_key is the primary key (column with unique values) for this table.
   *
   * Write a solution to report the customer ids from the Customer table
   * that bought all the products in the Product table.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/customers-who-bought-all-products/description/
   * */

  val session = sparkReadPGTable("CustomersWhoBoughtAllProducts")
  val customerDF = session("customer_1045")
  val productDF = session("product_1045")
  val windowSpec = Window.partitionBy("customer_id").orderBy("product_key")

  customerDF.join(productDF.withColumn("products_cnt", F.count("product_key").over()), "product_key")
    .withColumn("bought_rnk", F.dense_rank().over(windowSpec))
    .filter(F.col("bought_rnk") === F.col("products_cnt"))
    .select("customer_id")
    .distinct()
    .show()
}
