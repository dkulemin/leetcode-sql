package medium

import org.apache.spark.sql.{SparkSession, functions}

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

  val spark = SparkSession.builder()
    .appName("MonthlyTransactionsI")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val transactionsDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "transactions")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  transactionsDf
    .withColumn("month", functions.date_format(functions.col("trans_date"), "yyyy-MM"))
    .groupBy("month", "country")
    .agg(
      functions.count("id").alias("trans_count"),
      functions.sum(
        functions.when(
          functions.col("state") === "approved", 1
        ).otherwise(0)
      ).alias("approved_count"),
      functions.sum("amount").alias("trans_total_amount"),
      functions.sum(functions.when(
        functions.col("state") === "approved", functions.col("amount")
      ).otherwise(0)).alias("approved_total_amount"),
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
