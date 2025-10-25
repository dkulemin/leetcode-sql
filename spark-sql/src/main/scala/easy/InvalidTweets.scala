package easy

import org.apache.spark.sql.{functions => F}
import utils.Utils.sparkReadPGTable

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

  val session = sparkReadPGTable("InvalidTweets")
  val tweetsDf = session("tweets")
  tweetsDf.filter(F.len(F.col("content")) > 15)
    .select("tweet_id")
    .show()
}
