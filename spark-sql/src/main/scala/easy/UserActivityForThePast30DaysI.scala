package easy

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

object UserActivityForThePast30DaysI extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("User_Activity_for_the_Past_30_Days_I")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "activity")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  println(
    df.
      filter(functions.col("activity_date") >= "2019-07-01" && functions.col("activity_date") <= "2019-07-27").
      groupBy(functions.col("activity_date")).
      agg(functions.countDistinct("user_id").alias("active_users")).
      show()
  )

}
