package easy

import org.apache.spark.sql.{SparkSession, functions}

object RecyclableAndLowFatProducts extends App {
  /**
   * +-------------+---------+
   * | Column Name | Type    |
   * +-------------+---------+
   * | product_id  | int     |
   * | low_fats    | enum    |
   * | recyclable  | enum    |
   * +-------------+---------+
   * product_id is the primary key (column with unique values) for this table.
   * low_fats is an ENUM (category) of type ('Y', 'N') where 'Y' means this product is low fat and 'N' means it is not.
   * recyclable is an ENUM (category) of types ('Y', 'N') where 'Y' means this product is recyclable and 'N' means it is not.
   *
   * Write a solution to find the ids of products that are both low fat and recyclable.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/recyclable-and-low-fat-products/description/
   */

  val spark = SparkSession.builder()
    .appName("RecyclableAndLowFatProducts")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "products")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  df.filter(functions.col("low_fats") === "Y" && functions.col("recyclable") === "Y")
    .select("product_id")
    .show()
}
