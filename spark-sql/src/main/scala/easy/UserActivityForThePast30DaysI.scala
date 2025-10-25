package easy

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

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

  val session = sparkReadPGTable("UserActivityForThePast30DaysI")
  val activityDf = session("activity")

  activityDf.filter(
      F.col("activity_date") > date.minusDays(30) &&
        F.col("activity_date") <= date
    )
    .groupBy(F.col("activity_date").alias("day"))
    .agg(F.countDistinct("user_id").alias("active_users"))
    .show()
}
