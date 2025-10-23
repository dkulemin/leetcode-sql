package easy

import org.apache.spark.sql.{SparkSession, functions}

object FindCustomerReferee extends App {
  /**
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | id          | int     |
   * | name        | varchar |
   * | referee_id  | int     |
   * +-------------+---------+
   * In SQL, id is the primary key column for this table.
   * Each row of this table indicates the id of a customer, their name, and the id of the customer who referred them.
   *
   * Find the names of the customer that are either:
   * 1. referred by any customer with id != 2.
   * 2. not referred by any customer.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/find-customer-referee/description/
   */

  val spark = SparkSession.builder()
    .appName("FindCustomerReferee")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "customer")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  df.filter(!(functions.col("referee_id") === 2) || functions.col("referee_id").isNull)
    .select("name")
    .show()
}
