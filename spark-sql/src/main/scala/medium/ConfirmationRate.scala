package medium

import org.apache.spark.sql.{SparkSession, functions}

object ConfirmationRate extends App {
  /**
   * Table: Signups
   *
   * +----------------+----------+
   * | Column Name    | Type     |
   * +----------------+----------+
   * | user_id        | int      |
   * | time_stamp     | datetime |
   * +----------------+----------+
   * user_id is the column of unique values for this table.
   * Each row contains information about the signup time for the user with ID user_id.
   *
   * Table: Confirmations
   *
   * +----------------+----------+
   * | Column Name    | Type     |
   * +----------------+----------+
   * | user_id        | int      |
   * | time_stamp     | datetime |
   * | action         | ENUM     |
   * +----------------+----------+
   * (user_id, time_stamp) is the primary key (combination of columns with unique values) for this table.
   * user_id is a foreign key (reference column) to the Signups table.
   * action is an ENUM (category) of the type ('confirmed', 'timeout')
   * Each row of this table indicates that the user with ID user_id requested a confirmation message at time_stamp
   * and that confirmation message was either confirmed ('confirmed') or expired without confirming ('timeout').
   *
   *
   * The confirmation rate of a user is the number of 'confirmed' messages divided by the total number of requested confirmation messages.
   * The confirmation rate of a user that did not request any confirmation messages is 0.
   * Round the confirmation rate to two decimal places.
   *
   * Write a solution to find the confirmation rate of each user.
   *
   * Return the result table in any order.
   *
   * https://leetcode.com/problems/confirmation-rate/description/
   * */

  val spark = SparkSession.builder()
    .appName("ArticleViewsI")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars", "./jars/postgresql-42.7.8.jar")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val signupsDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "signups")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  val confirmationsDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://127.0.0.1:5432/leetcodedb")
    .option("dbtable", "confirmations")
    .option("user", "leetcodeuser")
    .option("password", "pgpwd4leetcode")
    .option("driver", "org.postgresql.Driver")
    .load()

  signupsDf.join(confirmationsDf, "user_id", "left")
    .groupBy("user_id")
    .agg(
      functions.round(
        functions.avg(
          functions.when(functions.col("action") === "confirmed", 1.0)
            .otherwise(0.0)
        ),
        2
      ).alias("confirmation_rate")
    )
    .select("user_id", "confirmation_rate")
    .show()

}
