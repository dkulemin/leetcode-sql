package medium

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object MonthlyTransactionsI extends App {
  /**
   * Table: Transactions
   *
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | id            | int     |
   * | country       | varchar |
   * | state         | enum    |
   * | amount        | int     |
   * | trans_date    | date    |
   * +---------------+---------+
   * id is the primary key of this table.
   * The table has information about incoming transactions.
   * The state column is an enum of type ["approved", "declined"].
   *
   *
   * Write an SQL query to find for each month and country, the number of transactions and their total amount, the number of approved transactions and their total amount.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/monthly-transactions-i/description/
   * */

  val session = sparkReadPGTable("MonthlyTransactionsI")
  val transactionsDf = session("transactions")

  transactionsDf
    .withColumn("month", F.date_format(F.col("trans_date"), "yyyy-MM"))
    .groupBy("month", "country")
    .agg(
      F.count("id").alias("trans_count"),
      F.sum(
        F.when(
          F.col("state") === "approved", 1
        ).otherwise(0)
      ).alias("approved_count"),
      F.sum("amount").alias("trans_total_amount"),
      F.sum(
        F.when(
          F.col("state") === "approved", F.col("amount")
        ).otherwise(0)
      ).alias("approved_total_amount"),
    )
    .select(
      "month",
      "country",
      "trans_count",
      "approved_count",
      "trans_total_amount",
      "approved_total_amount"
    )
    .show()
}
