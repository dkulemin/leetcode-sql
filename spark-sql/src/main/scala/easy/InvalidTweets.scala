package easy

import org.apache.spark.sql.{SparkSession, functions}

object InvalidTweets extends App {
  /**
   * +----------------+---------+
   * | Column Name    | Type    |
   * +----------------+---------+
   * | tweet_id       | int     |
   * | content        | varchar |
   * +----------------+---------+
   * tweet_id is the primary key (column with unique values) for this table.
   * content consists of alphanumeric characters, '!', or ' ' and no other special characters.
   * This table contains all the tweets in a social media app.
   *
   *
   * Write a solution to find the IDs of the invalid tweets.
   * The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/invalid-tweets/description/
   * */

  val spark = SparkSession.builder()
    .appName("InvalidTweets")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "tweets")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  df.filter(functions.len(functions.col("content")) > 15)
    .select("tweet_id")
    .show()
}
