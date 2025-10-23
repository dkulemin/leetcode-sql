package easy

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.{SparkSession, functions}

object UserActivityForThePast30DaysI extends App {
  /**
   * +---------------+---------+
   * | Column Name   | Type    |
   * +---------------+---------+
   * | user_id       | int     |
   * | session_id    | int     |
   * | activity_date | date    |
   * | activity_type | enum    |
   * +---------------+---------+
   * This table may have duplicate rows.
   * The activity_type column is an ENUM (category) of type ('open_session', 'end_session', 'scroll_down', 'send_message').
   * The table shows the user activities for a social media website.
   * Note that each session belongs to exactly one user.
   *
   * Write a solution to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively.
   * A user was active on someday if they made at least one activity on that day.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/user-activity-for-the-past-30-days-i/description/
   */

  val dateStr = "2019-07-27"
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val date = LocalDate.parse(dateStr, formatter)

  val spark = SparkSession.builder()
    .appName("UserActivityForThePast30DaysI")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "activity")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  df.filter(functions.col("activity_date") > date.minusDays(30) && functions.col("activity_date") <= date)
    .groupBy(functions.col("activity_date").alias("day"))
    .agg(functions.countDistinct("user_id").alias("active_users"))
    .show()
}
