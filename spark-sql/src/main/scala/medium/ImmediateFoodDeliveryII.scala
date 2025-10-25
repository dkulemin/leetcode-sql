package medium

import utils.Utils.sparkReadPGTable
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window

object ImmediateFoodDeliveryII extends App {
  /**
   * Table: Delivery
   *
   * +-----------------------------+---------+
   * | Column Name                 | Type    |
   * +-----------------------------+---------+
   * | delivery_id                 | int     |
   * | customer_id                 | int     |
   * | order_date                  | date    |
   * | customer_pref_delivery_date | date    |
   * +-----------------------------+---------+
   * delivery_id is the column of unique values of this table.
   * The table holds information about food delivery to customers that make orders at some date and specify a preferred delivery date (on the same order date or after it).
   *
   * If the customer's preferred delivery date is the same as the order date, then the order is called immediate; otherwise, it is called scheduled.
   *
   * The first order of a customer is the order with the earliest order date that the customer made. It is guaranteed that a customer has precisely one first order.
   *
   * Write a solution to find the percentage of immediate orders in the first orders of all customers, rounded to 2 decimal places.
   *
   * https://leetcode.com/problems/immediate-food-delivery-ii/description/
   * */

  val session = sparkReadPGTable("ImmediateFoodDeliveryII")
  val deliveryDF = session("delivery")
  val windowSpec = Window.partitionBy("customer_id").orderBy("order_date")
  deliveryDF.withColumn("rn", F.row_number().over(windowSpec))
    .filter(F.col("rn") === 1)
    .agg(
      F.round(
        F.avg(
          F.when(
            F.col("order_date") === F.col("customer_pref_delivery_date"),
            1
          ).otherwise(0)
        ).multiply(100),
        2
      ).alias("immediate_percentage")
    )
    .select("immediate_percentage")
    .show()
}
