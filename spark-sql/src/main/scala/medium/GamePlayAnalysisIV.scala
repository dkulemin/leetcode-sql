package medium

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.expressions.Window
import utils.Utils.sparkReadPGTable

object GamePlayAnalysisIV extends App {
  /**
   * Table: Activity
   *
   * +--------------+---------+
   * | Column Name  | Type    |
   * +--------------+---------+
   * | player_id    | int     |
   * | device_id    | int     |
   * | event_date   | date    |
   * | games_played | int     |
   * +--------------+---------+
   * (player_id, event_date) is the primary key (combination of columns with unique values) of this table.
   * This table shows the activity of players of some games.
   * Each row is a record of a player who logged in and played a number of games (possibly 0)
   * before logging out on someday using some device.
   *
   * Write a solution to report the fraction of players that logged in again on the day
   * after the day they first logged in, rounded to 2 decimal places.
   * In other words, you need to determine the number of players who logged in on the day
   * immediately following their initial login, and divide it by the number of total players.
   *
   * https://leetcode.com/problems/game-play-analysis-iv/description/
   * */

  val session = sparkReadPGTable("GamePlayAnalysisIV")
  val activityDF = session("player_activity")
  val windowSpec = Window.partitionBy("player_id").orderBy("event_date")
  activityDF.withColumns(Map(
      "first_event_date" -> F.first_value(F.col("event_date")).over(windowSpec),
      "event_date-1" -> F.date_sub(F.col("event_date"), 1)
    ))
    .agg(
      (F.sum(F.when(F.col("first_event_date") === F.col("event_date-1"), 1.0).otherwise(0.0))
        / F.countDistinct("player_id")).alias("fraction")
    )
    .show()

}
