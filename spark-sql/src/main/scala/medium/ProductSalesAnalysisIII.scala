package medium

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import utils.Utils.sparkReadPGTable

object ProductSalesAnalysisIII extends App {
  /**
   * Table: Sales
   *
   * +-------------+-------+
   * | Column Name | Type  |
   * +-------------+-------+
   * | sale_id     | int   |
   * | product_id  | int   |
   * | year        | int   |
   * | quantity    | int   |
   * | price       | int   |
   * +-------------+-------+
   * (sale_id, year) is the primary key (combination of columns with unique values) of this table.
   * Each row records a sale of a product in a given year.
   * A product may have multiple sales entries in the same year.
   * Note that the per-unit price.
   *
   * Write a solution to find all sales that occurred in the first year each product was sold.
   *
   * For each product_id, identify the earliest year it appears in the Sales table.
   *
   * Return all sales entries for that product in that year.
   *
   * Return a table with the following columns: product_id, first_year, quantity, and price.
   * Return the result in any order.
   * https://leetcode.com/problems/product-sales-analysis-iii/description/
   * */

  val session = sparkReadPGTable("ProductSalesAnalysisIII")
  val salesDF = session("sales")
  val windowSpec = Window.partitionBy("product_id").orderBy("year")
  salesDF
    .withColumn("first_year", F.first_value(F.col("year")).over(windowSpec))
    .filter(F.col("year") === F.col("first_year"))
    .select(
      "product_id",
      "first_year",
      "quantity",
      "price"
    )
    .show()
}
