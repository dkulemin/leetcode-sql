package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

object FindFollowersCount extends App {
  /**
   * Table: Followers
   *
   * +-------------+------+
   * | Column Name | Type |
   * +-------------+------+
   * | user_id     | int  |
   * | follower_id | int  |
   * +-------------+------+
   * (user_id, follower_id) is the primary key (combination of columns with unique values) for this table.
   * This table contains the IDs of a user and a follower in a social media app where the follower follows the user.
   *
   *
   * Write a solution that will, for each user, return the number of followers.
   *
   * Return the result table ordered by user_id in ascending order.
   *
   * https://leetcode.com/problems/find-followers-count/description/
   * */

  sparkReadPGTable("FindFollowersCount")("followers")
    .groupBy("user_id")
    .agg(F.count("follower_id").alias("followers_count"))
    .show()
}
