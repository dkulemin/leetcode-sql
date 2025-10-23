package easy

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.{SparkSession, functions}

object UserActivityForThePast30DaysI extends App {
  /**
   * Write a solution to find the daily active user count for a period of 30 days ending 2019-07-27 inclusively.
   * A user was active on someday if they made at least one activity on that day.
   *
   * Return the result table in any order.
   *
   * The result format is in the following example.
   *
   * https://leetcode.com/problems/user-activity-for-the-past-30-days-i/description/
   */

  val dateStr = "2019-07-27"
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val date = LocalDate.parse(dateStr, formatter)

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
