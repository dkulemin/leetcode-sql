package easy

import org.apache.spark.sql.{SparkSession, functions}

object ArticleViewsI extends App {
  /**
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | article_id    | int     |
   * | author_id     | int     |
   * | viewer_id     | int     |
   * | view_date     | date    |
   * +---------------+---------+
   * There is no primary key (column with unique values) for this table, the table may have duplicate rows.
   * Each row of this table indicates that some viewer viewed an article (written by some author) on some date.
   * Note that equal author_id and viewer_id indicate the same person.
   *
   * Write a solution to find all the authors that viewed at least one of their own articles.
   *
   * Return the result table sorted by id in ascending order.
   *
   * https://leetcode.com/problems/article-views-i/description/
   * */

  val spark = SparkSession.builder()
    .appName("User_Activity_for_the_Past_30_Days_I")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "views")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  df.filter(functions.col("author_id") === functions.col("viewer_id"))
    .select(functions.col("author_id").alias("id"))
    .sort()
    .distinct()
    .show()
}
